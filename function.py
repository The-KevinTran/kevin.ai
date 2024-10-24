"""
title: SQL Val Query
author: Maanas Manoj
version: 1.1
requirements: llama_index, sqlalchemy, mysql-connector, psycopg2-binary, llama_index.llms.ollama
"""

from pydantic import BaseModel, Field
from typing import Union, Generator, Iterator, Optional
import os
import sqlite3

import requests
from langchain_community.utilities.sql_database import SQLDatabase
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from sqlalchemy import create_engine
from open_webui.utils.misc import get_last_user_message
from langchain_aws import ChatBedrock
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_community.tools.sql_database.tool import (
    InfoSQLDatabaseTool,
    ListSQLDatabaseTool,
    QuerySQLCheckerTool,
    QuerySQLDataBaseTool,
)
from langchain import hub
from langgraph.prebuilt import create_react_agent
import os.path
from pathlib import Path
import time
from collections import deque
from datetime import datetime, timedelta
import threading

# If you need to see where the SQL query is failing, uncomment the line below
# llama_index.core.set_global_handler("simple")


class RateLimitedChatBedrock(ChatBedrock):
    """
    A rate-limited version of ChatBedrock that ensures no more than N
    API calls are made within any 60 second window.
    """

    def __init__(self, max_calls_per_minute: int = 4, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._max_calls = max_calls_per_minute
        self._calls = deque()  # Stores timestamps of API calls
        self._lock = threading.Lock()  # Thread-safe lock for managing calls deque

    def _enforce_rate_limit(self):
        """Enforces the rate limit by waiting if necessary."""
        current_time = datetime.now()

        with self._lock:
            # Remove timestamps older than 1 minute
            while self._calls and current_time - self._calls[0] > timedelta(minutes=1):
                self._calls.popleft()

            # If we've reached the rate limit, wait until we can make another call
            if len(self._calls) >= self._max_calls:
                wait_time = (
                    self._calls[0] + timedelta(minutes=1) - current_time
                ).total_seconds()
                if wait_time > 0:
                    time.sleep(wait_time)
                    # After waiting, remove old timestamps again
                    current_time = datetime.now()
                    while self._calls and current_time - self._calls[0] > timedelta(
                        minutes=1
                    ):
                        self._calls.popleft()

            # Add current timestamp to the queue
            self._calls.append(current_time)

    def invoke(self, *args, **kwargs):
        """Override invoke to add rate limiting."""
        self._enforce_rate_limit()
        return super().invoke(*args, **kwargs)

    def generate(self, *args, **kwargs):
        """Override generate to add rate limiting."""
        self._enforce_rate_limit()
        return super().generate(*args, **kwargs)


class Pipe:
    class Valves(BaseModel):
        DB_ENGINE: str = Field(
            os.getenv("DB_TYPE", "postgres"),
            description="Database type (supports 'postgres' and 'mysql', defaults to postgres)",
        )
        DB_HOST: str = Field(
            os.getenv("DB_HOST", "localhost"), description="Database hostname"
        )
        DB_PORT: str = Field(
            os.getenv("DB_PORT", "5432"), description="Database port (default: 5432)"
        )
        DB_USER: str = Field(
            os.getenv("DB_USER", "postgres"),
            description="Database user to connect with. Make sure this user has permissions to the database and tables you define",
        )
        DB_PASSWORD: str = Field(
            os.getenv("DB_PASSWORD", "password"), description="Database user's password"
        )
        DB_DATABASE: str = Field(
            os.getenv("DB_DATABASE", "postgres"),
            description="Database with the data you want to ask questions about",
        )
        DB_TABLE: str = Field(
            os.getenv("DB_TABLE", "table_name"),
            description="Table in the database with the data you want to ask questions about",
        )
        OLLAMA_HOST: str = Field(
            os.getenv("OLLAMA_HOST", "http://host.docker.internal:11434"),
            description="Hostname of the Ollama host with the model",
        )
        TEXT_TO_SQL_MODEL: str = Field(
            os.getenv("TEXT_TO_SQL_MODEL", "phi3:latest"),
            description="LLM model to use for text-to-SQL operation. Note that some models fail at SQL generation, such as llama3.2",
        )
        CONTEXT_WINDOW: int = Field(
            os.getenv("CONTEXT_WINDOW", 100000),
            description="The number of tokens to use in the context window",
        )

        class Config:
            arbitrary_types_allowed = True

        pass

    def __init__(self):
        self.valves = self.Valves()
        self.name = "Database RAG Pipeline"
        self.engine = None
        self.nlsql_response = ""
        engine = self.get_engine_for_chinook_db()
        db = SQLDatabase(engine)
        sqlite_uri = "sqlite:///./data/players.db"
        db = SQLDatabase.from_uri(sqlite_uri)

        # Set up LLM connection; uses phi3 model with 128k context limit since some queries have returned 20k+ tokens
        llm = RateLimitedChatBedrock(
            max_calls_per_minute=5,
            model="anthropic.claude-3-5-sonnet-20240620-v1:0",
            temperature=0,
            max_tokens=None,
            region_name="us-west-2",
            # other params...
        )

        toolkit = SQLDatabaseToolkit(db=db, llm=llm)
        prompt_template = hub.pull("langchain-ai/sql-agent-system-prompt")
        system_message = """System: 
        Valorant Knowledge: {
        The following are agents, or characters in the Valorant game, as well as the agent role they fall under. They are not to be confused with players, who are pro players of the game. {
        Duelist: [jett, yoru, reyna, phoenix, raze, neon, iso]. 
        Scan Initiator: [sova, fade, skye]. 
        Flash Initiator: [skye, breach, kayo, gekko]. 
        Controller: [brim, viper, omen, astra , harbor, clove]. 
        Sentinel: [cypher, killjoy, sage, deadlock, chamber].
        }

        If asked to create a team, follow these guidelines: {
            - ONLY pick players from the database.
            - For the attributes I have referenced below in quotes, you can find them by querying the Players and Agents table schema.
            - Each team MUST have one IGL, or in game leader. When querying the Players table in the database, the player's field "igl" will equal "true" if they have experience being an IGL. IGL's may play any agent.
            - You must pick 5 players total for every time including an IGL.
            - When creating a team, first filter the player pool to choose from by the league that the user has requested. If they don't specify a league, pick from any league. Each player has a "player_league" attribute in the Players database table. The possible league values in the database are ["VCT-International", "VCT-Game-Changers", "VCT-Challengers"]
            - When selecting a player for a specific agent role, search the player's Agents table rows by using their "player_id", and use their top 3 most played agents (using "games_played") to determine if they are a good fit for that agent role. If they do not have at least one agent belonging to the role in their top 3 most played agents, DO NOT pick them for the role. Include the agents they would play in your final response.
            - There exists agent data for almost every player, so double check there isn't any agents associated with the current "player_id" before saying there isn't agent data.
            - Unless you determine a statistic that is best for the agent role you are picking, use "total_score" to compare players.
        }
        
        
        Example meta composition: {[jett, omen, killjoy, sova, kayo] -> [duelist, controller, sentinel, initiator, initiator]}
        Example meta composition: {[raze, brimstone, skye, gecko, viper] -> [duelist, controller, initiator, initiator, controller]}
        Example meta composition: {[raze, omen, cypher, gecko, kayo/sova] -> [duelist, controller, sentinel, initiator, initiator]}
        Example meta composition: {[neon, astra, chamber, fade, yoru] -> [duelist, controller, sentinel, initiator, duelist]}
        Example meta composition: {[jett, viper, sova, cypher, kayo/skye] -> [duelist, controller, initiator, sentinel, initiator]}
        Example meta composition: {[jett, astra, cypher, kayo, sova] -> [duelist, controller, sentinel, initiator, initiator]}
        }
        Instructions: {You are an agent designed to provide information about Valorant teams/pro players, and interact with a SQL database which contains information about pro players.\nGiven an input question, determine whether it requires querying the SQLite database or can be answered with your limited Valorant knowledge.\n\nIf it requires querying the database, create a syntactically correct SQLite query to run, then look at the results of the query and return the answer.\nUnless the user specifies a specific number of examples they wish to obtain, always limit your query to at most 5 results.\nYou can order the results by a relevant column to return the most interesting examples in the database. Use JSON format for data. \nNever query for all the columns from a specific table, only ask for the relevant columns given the question.\nYou have access to tools for interacting with the database.\nOnly use the below tools. If you decide to query the database, only use the information returned by the below tools to construct your final answer.\nYou MUST double check your query before executing it. If you get an error while executing a query, rewrite the query and try again.\n\nDO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database.\n\nTo start you should ALWAYS query the schema of the most relevant tables to see what information you have.\nDo NOT skip this step.\nIf the user's question can be answered using information from the tables, use it. Otherwise, answer using your best judgement.}"""

        self.agent_executor = create_react_agent(
            llm, toolkit.get_tools(), state_modifier=system_message
        )
        pass

    def get_provider_models(self):
        return [
            {
                "id": "anthropic.claude-3-5-sonnet-20240620-v1:0",
                "name": "anthropic.claude-3-5-sonnet-20240620-v1:0",
            }
        ]

    def get_engine_for_chinook_db(self):
        """Pull sql file, populate in-memory database, and create engine."""
        print("DB EXISTS:", os.path.abspath(os.getcwd()))
        print("File      Path:", Path(__file__).absolute())
        connection = sqlite3.connect("data/players.db", check_same_thread=False)
        return create_engine(
            "sqlite://",
            creator=lambda: connection,
            poolclass=StaticPool,
            connect_args={"check_same_thread": False},
        )

    def pipe(
        self,
        body: dict,
        __user__: dict,
        __event_emitter__=None,
        __event_call__=None,=
        __valves__=None,
    ) -> Union[str, Generator, Iterator]:

        print(f"pipe:{__name__}")

        print(__event_emitter__)
        print(__event_call__)

        user_message = get_last_user_message(body["messages"])

        response = self.agent_executor.invoke({"messages": body["messages"]})[
            "messages"
        ]
        print(response)

        return response[-1].content
