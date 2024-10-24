import json

def load_json_file(filename):
    """Load JSON data from a file."""
    with open(filename, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_json_file(data, filename):
    """Save JSON data to a file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

def print_data_structure(data, filename):
    """Debug function to print the structure of the loaded data."""
    print(f"\nStructure of {filename}:")
    print(f"Type: {type(data)}")
    if isinstance(data, dict):
        print("Keys:", list(data.keys()))
    elif isinstance(data, list):
        print(f"Length: {len(data)}")
        if len(data) > 0:
            print("First item type:", type(data[0]))
            if isinstance(data[0], dict):
                print("First item keys:", list(data[0].keys()))
            print("\nFirst item:")
            print(json.dumps(data[0], indent=2))

def filter_challengers_players(challengers_data, international_data, gamechangers_data):
    """
    Filter out players from challengers who appear in international or gamechangers.
    Uses player_link as the unique identifier.
    """
    # Ensure we're working with lists of players
    if isinstance(challengers_data, dict):
        challengers_data = challengers_data.get('players', [])
    if isinstance(international_data, dict):
        international_data = international_data.get('players', [])
    if isinstance(gamechangers_data, dict):
        gamechangers_data = gamechangers_data.get('players', [])

    try:
        # Create sets of player links from international and gamechangers
        international_links = {player['player_link'] for player in international_data}
        gamechangers_links = {player['player_link'] for player in gamechangers_data}
        
        # Track removed players
        removed_international = []
        removed_gamechangers = []
        
        # Check each challenger player
        filtered_challengers = []
        for player in challengers_data:
            if player['player_link'] in international_links:
                removed_international.append({
                    'name': player.get('player_name', 'Unknown'),
                    'link': player['player_link'],
                    'team': player.get('player_team_initials', 'Unknown')
                })
            elif player['player_link'] in gamechangers_links:
                removed_gamechangers.append({
                    'name': player.get('player_name', 'Unknown'),
                    'link': player['player_link'],
                    'team': player.get('player_team_initials', 'Unknown')
                })
            else:
                filtered_challengers.append(player)
        
        # Print statistics
        print("\n=== Filtering Statistics ===")
        print(f"Original challengers players: {len(challengers_data)}")
        print(f"Players after filtering: {len(filtered_challengers)}")
        print(f"Total players removed: {len(challengers_data) - len(filtered_challengers)}")
        
        # Print removed international players
        print(f"\n=== Removed International Players ({len(removed_international)}) ===")
        for player in removed_international:
            print(f"- {player['name']} ({player['team']}) - {player['link']}")
        
        # Print removed gamechangers players
        print(f"\n=== Removed Gamechangers Players ({len(removed_gamechangers)}) ===")
        for player in removed_gamechangers:
            print(f"- {player['name']} ({player['team']}) - {player['link']}")
        
        return filtered_challengers
    except (KeyError, TypeError) as e:
        print(f"Error accessing data structure: {e}")
        print("Please check if 'player_link' exists in all player objects")
        return []

def main():
    try:
        # Load the JSON files
        challengers_data = load_json_file('players_challengers.json')
        international_data = load_json_file('players_international.json')
        gamechangers_data = load_json_file('players_gamechangers.json')
        
        # Print debug information about the structure of each file
        print_data_structure(challengers_data, 'players_challengers.json')
        print_data_structure(international_data, 'players_international.json')
        print_data_structure(gamechangers_data, 'players_gamechangers.json')
        
        # Filter the data
        filtered_data = filter_challengers_players(
            challengers_data,
            international_data,
            gamechangers_data
        )
        
        if filtered_data:
            # Save the filtered data
            save_json_file(filtered_data, 'filtered-challengers.json')
            print("\nFiltered data has been saved to 'filtered-challengers.json'")
        
    except FileNotFoundError as e:
        print(f"Error: Could not find one of the required JSON files: {e}")
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format in one of the files: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()