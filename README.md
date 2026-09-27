# Arcus Checker

Static Arcus rank/PnL/fees/volume checker.

## Run
`python3 -m http.server 8080`

Then open `http://localhost:8080`.

## API
`https://api.arcus.xyz/v1/leaderboard?window=all|30d|24h&sortBy=volume&address=<wallet>`

No private keys, signatures, or wallet connection are used.

The app reports the Arcus **trading leaderboard** rank. It does not claim to report a season/points rank.

The UI currently divides returned volume, fees and PnL integers by 1e6 for display. Verify this unit convention if Arcus changes its API.
