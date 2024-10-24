import json
import requests
from lxml import html
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime
import time
from typing import List, Dict
import os

def load_json(file_path):
    with open(file_path, 'r', encoding="utf8") as file:
        return json.load(file)

def save_json(data, file_path):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=2)

def find_parent_a_tag_href(html_content, child_id):
    soup = BeautifulSoup(html_content, 'html.parser')
    child_element = soup.find(id=child_id)
    if not child_element:
        return None
    parent_a_tag = child_element.find_parent('a')
    if parent_a_tag and 'href' in parent_a_tag.attrs:
        return parent_a_tag['href']
    else:
        return None

def get_href_by_xpath(url, xpath):
    try:
        response = requests.get(url)
        response.raise_for_status()
        tree = html.fromstring(response.content)
        element = tree.xpath(xpath)
        if element:
            href = element[0].get('href')
            return href
        else:
            return "Element not found"
    except requests.exceptions.RequestException as e:
        return f"Error fetching the webpage: {str(e)}"
    except Exception as e:
        return f"An error occurred: {str(e)}"

def get_parent_info_by_title(url, title):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        element = soup.find(attrs={"title": title})
        if element:
            result = {}
            if element.parent:
                result["parent_text"] = element.parent.get_text(strip=True)
            else:
                result["parent_text"] = "Parent not found"
            ancestor_a = element.find_parent('a')
            if ancestor_a and ancestor_a.has_attr('href'):
                result["ancestor_href"] = ancestor_a['href']
            else:
                result["ancestor_href"] = "No ancestor <a> tag with href found"
            return result
        else:
            return "Element not found"
    except requests.exceptions.RequestException as e:
        return f"Error fetching the webpage: {str(e)}"
    except Exception as e:
        return f"An error occurred: {str(e)}"

def get_parent_text_by_title(url, title):
    try:
        response = requests.get(url)
        response.raise_for_status()
        tree = html.fromstring(response.content)
        xpath = f"//*[@title='{title}']/parent::*"
        parent_element = tree.xpath(xpath)
        if parent_element:
            parent_text = parent_element[0].text_content().strip()
            return parent_text
        else:
            return "Element or parent not found"
    except requests.exceptions.RequestException as e:
        return f"Error fetching the webpage: {str(e)}"
    except Exception as e:
        return f"An error occurred: {str(e)}"

def scrape_player_data(player_link, xpath):
    response = requests.get(player_link)
    tree = html.fromstring(response.content)
    table = tree.xpath(xpath)
    if not table:
        return None
    agents_data = []
    for row in table[0].xpath('.//tr'):
        cells = row.xpath('.//td')
        if len(cells) < 4:
            continue
        agent_img = cells[0].xpath('.//img')
        if not agent_img or 'alt' not in agent_img[0].attrib:
            continue
        agent_name = agent_img[0].attrib['alt']
        usage_text = cells[1].text_content().strip()
        games_played = int(usage_text.split()[0].strip('()'))
        agent_data = {
            agent_name: {
                "games_played": games_played,
                "rnd": cells[2].text_content().strip(),
                "rating": cells[3].text_content().strip(),
                "acs": cells[4].text_content().strip(),
                "kd": cells[5].text_content().strip(),
                "adr": cells[6].text_content().strip(),
                "kast": cells[7].text_content().strip(),
                "kpr": cells[8].text_content().strip(),
                "apr": cells[9].text_content().strip(),
                "fkpr": cells[10].text_content().strip(),
                "fdpr": cells[11].text_content().strip(),
                "k": cells[12].text_content().strip(),
                "d": cells[13].text_content().strip(),
                "a": cells[14].text_content().strip(),
                "fk": cells[15].text_content().strip(),
                "fd": cells[16].text_content().strip(),
            }
        }
        agents_data.append(agent_data)
    return {"agents": agents_data}

def process_players(input_file, xpath):
    data = load_json(input_file)
    for player in data['players']:
        player_link = "https://" + player['player_link'] + "/?timespan=all"
        print(player_link)
        player_data = scrape_player_data(player_link, xpath)
        if player_data:
            player['agents'] = player_data['agents']
        time.sleep(.1)
    return data

def calculate_rating_score(rating, is_igl):
    try:
        rating = float(rating)
    except ValueError:
        return None
    if 1.30 <= rating <= 1.40:
        score = 30
    elif 1.20 <= rating <= 1.29:
        score = 27 + int((rating - 1.20) / 0.01)
    elif 1.10 <= rating <= 1.19:
        score = 24 + int((rating - 1.10) / 0.01)
    elif 1.00 <= rating <= 1.09:
        score = 21 + int((rating - 1.00) / 0.01)
    elif 0.90 <= rating <= 0.99:
        score = 18 + int((rating - 0.90) / 0.01)
    elif 0.80 <= rating <= 0.89:
        score = 15 + int((rating - 0.80) / 0.01)
    elif 0.70 <= rating <= 0.79:
        score = 12 + int((rating - 0.70) / 0.01)
    elif 0.60 <= rating <= 0.69:
        score = 9 + int((rating - 0.60) / 0.01)
    elif 0.50 <= rating <= 0.59:
        score = 6 + int((rating - 0.50) / 0.01)
    elif 0.40 <= rating <= 0.49:
        score = 3 + int((rating - 0.40) / 0.01)
    elif 0 <= rating <= 0.39:
        score = min(2, max(0, int(rating / 0.195)))
    else:
        score = 0
    if is_igl:
        score += 5
    return min(score, 30)

def calculate_agent_flexibility(agents):
    roles = {
        "Duelist": ["Phoenix", "Reyna", "Jett", "Raze", "Yoru", "Neon", "Iso"],
        "Sentinel": ["Sage", "Cypher", "Killjoy", "Chamber", "Deadlock", "Vyse"],
        "Initiator": ["Sova", "Breach", "Skye", "Kayo", "Fade", "Gekko"],
        "Controller": ["Brimstone", "Viper", "Omen", "Astra", "Harbor", "Clove"]
    }
    played_agents = set()
    role_coverage = set()
    total_rating = 0
    agent_count = 0
    for agent_data in agents:
        agent_name = list(agent_data.keys())[0]
        agent_info = agent_data[agent_name]
        if 'rating' not in agent_info or agent_info['rating'] == "":
            continue
        try:
            agent_rating = float(agent_info['rating'])
        except ValueError:
            continue
        played_agents.add(agent_name)
        agent_count += 1
        total_rating += agent_rating
        for role, agent_list in roles.items():
            if agent_name in agent_list:
                role_coverage.add(role)
                break
    flexibility_score = len(played_agents) + len(role_coverage)
    if agent_count > 0:
        avg_rating = total_rating / agent_count
        scaled_rating = (avg_rating / 1.4) * 6
        flexibility_score += scaled_rating
    return round(flexibility_score, 2)

def scrape_player_scores(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    h2 = soup.find('h2', string=lambda text: 'Event Placements' in text if text else False)
    if not h2:
        return "H2 'Event Placements' not found."
    div = h2.find_next('div')
    if not div:
        return "Div after 'Event Placements' h2 not found."
    highest_score = float('-inf')
    current_year = datetime.now().year
    for a_tag in div.find_all('a', class_='player-event-item'):
        year_div = a_tag.find('div', string=lambda text: text and text.strip().isdigit())
        if not year_div or int(year_div.text.strip()) != current_year:
            continue
        placement_span = a_tag.find('span', class_='ge-text-light')
        if not placement_span:
            continue
        tournament_name = a_tag.find('div', class_='text-of').text.strip() + placement_span.text.strip().split('–')[0].strip()
        placement = placement_span.text.strip().split('–')[-1].strip()
        score = calculate_score(tournament_name, placement)
        highest_score = max(highest_score, score)
    if highest_score != float('-inf'):
        return highest_score
    else:
        return 0

def calculate_score(tournament_name, placement):
    if "Valorant Champions" in tournament_name or ("Champions Tour" in tournament_name and "Masters" in tournament_name):
        if placement == "1st":
            return 30
        else:
            return 25
    elif "Champions Tour" in tournament_name and ("Stage 1" in tournament_name or "Stage 2" in tournament_name):
        if "Playoffs" in tournament_name:
            if not (placement in ["1st", "2nd", "3rd"]):
                return 20
        else:
            return 15
    elif "Challengers League" in tournament_name:
        if placement in ["1st", "2nd", "3rd"]:
            return 10
        else:
            return 5
    elif "Game Changers Championship" in tournament_name:
        return 10
    return 0

def process_json(input_json, output_json):
    data = load_json(input_json)
    for player in data['players']:
        player_name = player['player_name']
        rating = player.get('rating', '')
        is_igl = player.get('role', '').lower() == 'igl'
        if rating == "" or 'agents' not in player or not player['agents']:
            print(f"Skipping player {player_name} due to missing data.")
            continue
        rating_score = calculate_rating_score(rating, is_igl)
        if rating_score is None:
            print(f"Skipping player {player_name} due to invalid rating: {rating}")
            continue
        agent_flexibility = calculate_agent_flexibility(player['agents'])
        time.sleep(0.1)
        experience_score = scrape_player_scores("https://" + player['player_link'])
        print(experience_score, player['player_link'])
        player['rating_score'] = round(rating_score, 2)
        player['agent_flexibility'] = round(agent_flexibility, 2)
        player['experience'] = round(experience_score, 2)
        player['total_score'] = round(rating_score + agent_flexibility + experience_score, 2)
    data['players'] = sorted(data['players'], key=lambda x: x.get('total_score', 0), reverse=True)
    save_json(data, output_json)
    print(f"JSON file '{output_json}' has been created successfully.")
    return data



def process_category(source_file, category, league):
    output_file = f'players_{category}.json'
    source_data = load_json(source_file)
    for player in source_data['players']:
        player['league'] = league
    save_json(source_data, output_file)
    print(f"Processed {category} players saved to {output_file}")
    return output_file

def combine_scored_files():
    """Combines all players_scored_{category}.json files into a single JSON file."""
    categories = ['international', 'gamechangers', 'challengers']
    all_players = []
    
    for category in categories:
        file_path = f'players_scored_{category}.json'
        try:
            data = load_json(file_path)
            all_players.extend(data['players'])
        except FileNotFoundError:
            print(f"Warning: {file_path} not found")
            continue
    
    # Sort all players by total_score
    all_players = sorted(all_players, key=lambda x: x.get('total_score', 0), reverse=True)
    
    # Save combined results
    combined_data = {'players': all_players}
    save_json(combined_data, 'players_scored_combined.json')
    print("All scored players combined into players_scored_combined.json")


# Main execution
def step1_process_initial_data(input_file, league):
    print(f"Step 1: Processing initial player data for {league}...")
    source_data = load_json(input_file)

    igls_data = []
    for player in source_data["players"]:
        time.sleep(0.1)
        player["role"] = ""
        player["league"] = league  # Add league information
        url = "https://" + player["player_link"]
        xpath = "//*[@id='wrapper']/div[1]/div/div[2]/div[1]/div[4]/a"
        result = get_href_by_xpath(url, xpath)
        if result != "Element or parent not found":
            url = "https://www.vlr.gg" + result
            title = "Team Captain"
            result = get_parent_info_by_title(url, title)
            if isinstance(result, dict):
                if player["player_link"] == "www.vlr.gg" + result['ancestor_href']:
                    print(f"IGL: {player['player_name']} ({league})")
                    player["role"] = "igl"
                    igls_data.append({
                        "player_name": player["player_name"],
                        "player_link": "www.vlr.gg" + result['ancestor_href'],
                        "league": league
                    })
                print(player["player_name"])
    
    save_json(source_data, input_file)
    
    # Update or create igls.json
    try:
        existing_igls = load_json("igls.json")
        existing_igls["players"].extend(igls_data)
    except FileNotFoundError:
        existing_igls = {"players": igls_data}
    
    save_json(existing_igls, "igls.json")
    print(f"Step 1 completed for {league}. Updated data saved to {input_file}")
    print(f"IGL data for {league} added to igls.json")

def step2_process_player_stats(input_file, xpath):
    print("Step 2: Processing player stats...")
    results = process_players(input_file, xpath)
    save_json(results, input_file)
    print("Step 2 completed. Updated data saved to", input_file)

def step3_calculate_final_scores(input_file, output_file):
    print("Step 3: Calculating final scores...")
    final_results = process_json(input_file, output_file)
    print("Step 3 completed. Final results saved to", output_file)
    return final_results

def main():
    categories = {
        'international': {
            'file': 'players_international.json',
            'league': 'VCT-International'
        },
        'gamechangers': {
            'file': 'players_gamechangers.json',
            'league': 'VCT-Game-Changers'
        },
        'challengers': {
            'file': 'players_challengers.json',
            'league': 'VCT-Challengers'
        }
    }
    
    xpath = '//*[@id="wrapper"]/div[1]/div/div[2]/div[1]/div[2]/div/table'

    print("Starting the player data processing pipeline...")

    # Clear existing igls.json if it exists
    if os.path.exists("igls.json"):
        os.remove("igls.json")

    # Process each category
    for category, info in categories.items():
        source_file = info['file']
        league = info['league']
        if os.path.exists(source_file):
            output_file = process_category(source_file, category, league)
            
            # Step 1: Process initial data
            step1_process_initial_data(output_file, league)

            # Step 2: Process player stats
            step2_process_player_stats(output_file, xpath)

            # Step 3: Calculate final scores
            final_output_file = f'players_scored_{category}.json'
            final_results = step3_calculate_final_scores(output_file, final_output_file)
            
            print(f"Processing completed for {category}. Final results saved to {final_output_file}")
        else:
            print(f"Warning: Source file for {category} not found: {source_file}")

    # Combine all scored files into one
    combine_scored_files()

    print("All categories processed and combined. Check players_scored_combined.json for final results.")

if __name__ == "__main__":
    main()