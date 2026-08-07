"""Demonstrate which Renko bricks pandas selects with iloc[-41:-1]."""

import pandas as pd


def main():
    # Bricks 1-40 are history; brick 41 is the newest breakout brick.
    df = pd.DataFrame(
        {
            "brick_number": range(1, 42),
            "high": range(101, 142),
            "low": range(91, 132),
        }
    )
    df.loc[df.index[-1], ["high", "low"]] = [1000, -1000]

    previous_40 = df.iloc[-41:-1]
    latest_brick = df.iloc[-1]

    print("Selected brick numbers:")
    print(previous_40["brick_number"].tolist())
    print(f"\nNumber of selected bricks: {len(previous_40)}")
    print(f"Last selected brick: {previous_40.iloc[-1]['brick_number']}")
    print(f"Latest brick: {latest_brick['brick_number']}")

    high40 = previous_40["high"].max()
    low40 = previous_40["low"].min()
    print(f"\nPrevious 40 high: {high40}")
    print(f"Previous 40 low: {low40}")
    print(f"Latest brick high: {latest_brick['high']}")
    print(f"Latest brick low: {latest_brick['low']}")

    assert len(previous_40) == 40
    assert previous_40["brick_number"].tolist() == list(range(1, 41))
    assert latest_brick["brick_number"] not in previous_40["brick_number"].values
    assert high40 != latest_brick["high"]
    assert low40 != latest_brick["low"]

    print("\nPASS: iloc[-41:-1] selected bricks 1-40 and excluded brick 41.")


if __name__ == "__main__":
    main()
