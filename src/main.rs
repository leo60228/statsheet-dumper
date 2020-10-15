use anyhow::{anyhow, Error, Result};
use async_std::fs;
use async_std::task;
use futures::stream::{FuturesUnordered, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use surf::Client;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GameUpdate {
    pub id: String,
    pub statsheet: String,
    pub away_team: String,
    pub home_team: String,

    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GameStatsheet {
    pub away_team_stats: String,
    pub home_team_stats: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TeamStatsheet {
    pub player_stats: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlayerStatsheet {
    pub id: String,
    pub player_id: String,
    pub team_id: String,

    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct StatsheetsReq {
    pub ids: String,
}

fn surf_error(error: surf::Error) -> Error {
    anyhow!("{}", error)
}

async fn write_player_statsheet(day: usize, stats: PlayerStatsheet) -> Result<()> {
    let mut path = PathBuf::from("out");
    path.push("players");
    path.push(&stats.player_id);
    fs::create_dir_all(&path).await?;
    path.push(&day.to_string());
    path.set_extension("json");
    println!("writing {}", path.display());
    fs::write(&path, &serde_json::to_string(&stats)?).await?;
    println!("written {}", path.display());
    Ok(())
}

async fn write_game(day: usize, game: GameUpdate) -> Result<()> {
    let mut path = PathBuf::from("out");
    path.push("games");
    path.push(&day.to_string());
    fs::create_dir_all(&path).await?;
    path.push(&game.home_team);
    path.set_extension("json");
    println!("writing day {}", day);
    fs::write(&path, &serde_json::to_string(&game)?).await?;
    println!("written day {}", day);
    Ok(())
}

async fn fetch_player_statsheets(
    client: Client,
    day: usize,
    team_stats: Vec<TeamStatsheet>,
) -> Result<()> {
    let player_ids = team_stats
        .iter()
        .flat_map(|x| &x.player_stats)
        .map(|x| &**x)
        .collect::<Vec<&str>>()
        .join(",");

    println!("fetching day {} player statsheets", day);
    let player_stats: Vec<PlayerStatsheet> = client
        .get("https://www.blaseball.com/database/playerSeasonStats")
        .query(&StatsheetsReq { ids: player_ids })
        .map_err(surf_error)?
        .await
        .map_err(surf_error)?
        .body_json()
        .await
        .map_err(surf_error)?;
    println!("received day {} player statsheets", day);

    println!("writing day {} player statsheets", day);
    let futures: FuturesUnordered<_> = player_stats
        .into_iter()
        .map(|x| task::spawn(write_player_statsheet(day, x)))
        .collect();
    futures.try_collect::<()>().await?;

    Ok(())
}

async fn fetch_day(client: Client, season: usize, day: usize) -> Result<()> {
    #[derive(Serialize)]
    struct Games {
        season: usize,
        day: usize,
    }

    println!("fetching day {}", day);
    let games: Vec<GameUpdate> = client
        .get("https://www.blaseball.com/database/games")
        .query(&Games { season, day })
        .map_err(surf_error)?
        .await
        .map_err(surf_error)?
        .body_json()
        .await
        .map_err(surf_error)?;
    println!("received day {}", day);

    let game_ids = games
        .iter()
        .map(|x| &*x.statsheet)
        .collect::<Vec<&str>>()
        .join(",");

    println!("fetching day {} game statsheets", day);
    let game_stats: Vec<GameStatsheet> = client
        .get("https://www.blaseball.com/database/gameStatsheets")
        .query(&StatsheetsReq { ids: game_ids })
        .map_err(surf_error)?
        .await
        .map_err(surf_error)?
        .body_json()
        .await
        .map_err(surf_error)?;
    println!("received day {} team statsheets", day);

    let team_ids = game_stats
        .iter()
        .flat_map(|x| vec![&*x.away_team_stats, &*x.home_team_stats])
        .collect::<Vec<&str>>()
        .join(",");

    println!("fetching day {} team statsheets", day);
    let team_stats: Vec<TeamStatsheet> = client
        .get("https://www.blaseball.com/database/teamStatsheets")
        .query(&StatsheetsReq { ids: team_ids })
        .map_err(surf_error)?
        .await
        .map_err(surf_error)?
        .body_json()
        .await
        .map_err(surf_error)?;
    println!("received day {} team statsheets", day);

    println!("fetching day {} player statsheets", day);
    println!("writing day {} games", day);
    let futures: FuturesUnordered<_> = team_stats
        .chunks(5)
        .map(|x| {
            let client = client.clone();
            task::spawn(fetch_player_statsheets(client, day, x.to_vec()))
        })
        .chain(games.into_iter().map(|x| task::spawn(write_game(day, x))))
        .collect();
    futures.try_collect::<()>().await?;
    println!("finished day {}", day);

    Ok(())
}

#[async_std::main]
async fn main() -> Result<()> {
    let season = env::args()
        .nth(1)
        .ok_or_else(|| anyhow!("Missing season!"))?
        .parse::<usize>()?
        - 1;
    let client = Client::new();
    let futures: FuturesUnordered<_> = (0..99)
        .map(|day| {
            let client = client.clone();
            task::spawn(fetch_day(client, season, day))
        })
        .collect();
    futures.try_collect::<()>().await?;
    Ok(())
}
