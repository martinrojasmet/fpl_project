from utils.postgres import get_last_understat_game_id, add_understat_games, add_understat_player_games
from utils.helpers import convert_stringdate_to_date

from airflow.decorators import task
from playwright.sync_api import sync_playwright
import re
import pandas as pd

base_url = "https://understat.com/match/"

def extract_players_data(table, understat_game_id, team):
    rows = table.locator("tr")
    player_rows = []
    for index in range(rows.count()):
        row = table.locator("tr").nth(index)

        name = row.locator("td").nth(1).inner_text()
        minutes_played = row.locator("td").nth(3).inner_text()
        shots = row.locator("td").nth(4).inner_text()
        goals = row.locator("td").nth(5).inner_text()
        key_passes = row.locator("td").nth(6).inner_text()
        assists = row.locator("td").nth(7).inner_text()
        xG_td = row.locator("td").nth(8).inner_text()
        xA_td = row.locator("td").nth(9).inner_text()
        xX_regex = r"([-+]?\d+\.\d+)"
        expected_goals = re.search(xX_regex, xG_td).group(0)
        expected_assists = re.search(xX_regex, xA_td).group(0)
        player_row = {
            "name": name,
            "understat_game_id": understat_game_id,
            "team": team,
            "minutes_played": minutes_played,
            "shots": shots,
            "goals": goals,
            "assists": assists,
            "expected_goals": expected_goals,
            "expected_assists": expected_assists,
            "key_passes": key_passes
        }
        player_rows.append(player_row)
    return pd.DataFrame(player_rows)

@task
def add_understat_data_task():
    with sync_playwright() as playwright:
        player_games_df = pd.DataFrame(columns=["name", "understat_game_id", "team", "minutes_played", "shots", "goals", "assists", "expected_goals", "expected_assists", "key_passes"])
        games_df = pd.DataFrame(columns=["understat_id", "date", "home", "away"])

        first_understat_game_id = get_last_understat_game_id() + 1
        last_undertat_game_id = first_understat_game_id + 750*6
        current_understat_game_id = first_understat_game_id

        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        # cookies = context.cookies()
        page = context.new_page()

        while current_understat_game_id <= last_undertat_game_id:
            page.goto(base_url + str(current_understat_game_id))
            error_404_is_visible = page.get_by_text("404 The page you requested").is_visible()
            if not error_404_is_visible:
                league = page.locator("ul.breadcrumb li").nth(1).inner_text()
                
                date_string = page.locator("ul.breadcrumb li").nth(2).inner_text()
                date = convert_stringdate_to_date(date_string)

                home = page.locator("div.roster.roster-home h3 a").inner_text()
                away = page.locator("div.roster.roster-away h3 a").inner_text()

                if league and league == "EPL":
                    new_game = {
                        "understat_id": current_understat_game_id,
                        "date": date,
                        "home": home,
                        "away": away
                    }
                    games_df = pd.concat([games_df, pd.DataFrame([new_game])], ignore_index=True)

                    table = page.locator("#match-rosters div table tbody").nth(0)
                    
                    for tab, team_name in [("home", home), ("away", away)]:
                        page.locator(f"label[for='team-{tab}']").click()

                        new_player_games = extract_players_data(table, current_understat_game_id, team_name)
                        player_games_df = pd.concat([player_games_df, new_player_games], ignore_index=True)

            current_understat_game_id += 1

        add_understat_games(games_df)
        add_understat_player_games(player_games_df)

        context.close()
        browser.close()
