#! ./venv/bin/python3

import httpx
import re
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from ratelimit import limits, sleep_and_retry
from time import sleep
from typing import Generator, Optional

from secrets.config import (
    API,
    DISCOGS_USER,
    FOLDERS,
    RANGES,
    SCOPES,
    SHEETS,
    SHEET_ID,
    TIMEOUT,
    TOKEN,
)


def get_google_credentials():
    credentials = None
    if os.path.exists("secrets/token.json"):
        credentials = Credentials.from_authorized_user_file(
            "secrets/token.json", SCOPES
        )
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "secrets/credentials.json", SCOPES
            )
            credentials = flow.run_local_server(port=0)
        with open("secrets/token.json", "w") as token:
            token.write(credentials.to_json())
    return credentials


RELEASE_ID = re.compile(r"release/(\d+)")


def parse_release_id(url: str) -> int:
    match = RELEASE_ID.search(url)
    assert match is not None
    return int(match.group(1))


def parse_year(s: str) -> Optional[int]:
    try:
        return int(s)
    except ValueError:
        return None


def pad(values: list[str], length: int) -> list[str]:
    return values + ([""] * (length - len(values)))


def get_sheet_releases(sheet: str) -> dict[str, tuple[str, str, Optional[int], str]]:
    service = build("sheets", "v4", credentials=get_google_credentials())
    result = (
        service.spreadsheets()  # type: ignore
        .values()
        .batchGet(spreadsheetId=SHEET_ID, ranges=[f"'{sheet}'!{r}" for r in RANGES])
        .execute()
    )
    valueRanges = result.get("valueRanges", [])
    releases = {}
    for row, (v1, v2) in enumerate(
        zip(valueRanges[0]["values"], valueRanges[1]["values"]), start=2
    ):
        artist, release, year, url = [v.strip() for v in (pad(v1, 3) + pad(v2, 1))]
        if url:
            releases[parse_release_id(url)] = (
                artist,
                release,
                parse_year(year),
                f"A{row}",
            )
        else:
            print("No Discogs URL:\n")
            print(f"{artist} - {release}")
    return releases


@sleep_and_retry
@limits(calls=1, period=1)
def call_api(client: httpx.Client, endpoint: str, params: dict | None = None) -> dict:
    if params is None:
        params = {}
    r = client.get(
        API + endpoint,
        params=params,
        headers={"Authorization": f"Discogs token={TOKEN}"},
        timeout=TIMEOUT,
    )
    calls_remaining = int(r.headers.get("X-Discogs-Ratelimit-Remaining", 0))
    if calls_remaining < 5:
        sleep(10)
    if not r.status_code == 200:
        raise Exception(f"GET {r.url} failed ({r.status_code})")
    return r.json()


def paginate(
    client: httpx.Client, endpoint: str, key: str, params: dict | None = None
) -> Generator[dict, None, None]:
    if params is None:
        params = {}
    params["page"] = 1
    params["per_page"] = 100
    while True:
        o = call_api(client, endpoint, params)
        for item in o[key]:
            yield item
        if params["page"] == o["pagination"]["pages"]:
            break
        else:
            params["page"] += 1


def get_discogs_releases(folder: str) -> dict[str, tuple[str, str, Optional[int]]]:
    releases = {}
    with httpx.Client() as client:
        for release in paginate(
            client,
            f"/users/{DISCOGS_USER}/collection/folders/{FOLDERS[folder]}/releases",
            key="releases",
        ):
            releases[release["id"]] = (
                " / ".join(
                    [
                        artist["name"]
                        for artist in release["basic_information"]["artists"]
                    ]
                ),
                release["basic_information"]["title"],
                release["basic_information"]["year"] or None,
            )
    return releases


def print_links(
    id: str, sheet: str, sheet_releases: dict[str, tuple[str, str, Optional[int], str]]
):
    print(f"https://www.discogs.com/release/{id}")
    print(
        f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit"
        + f"#gid={SHEETS[sheet]}&range={sheet_releases[id][3]}"
    )


def compare(sheet, folder):
    print(f"\n{sheet} -------------------------\n")

    sheet_releases = get_sheet_releases(sheet)
    discogs_releases = get_discogs_releases(folder)

    sheet_ids = set(sheet_releases.keys())
    discogs_ids = set(discogs_releases.keys())

    different_years = set()
    for id in sheet_ids & discogs_ids:
        if not sheet_releases[id][2] == discogs_releases[id][2]:
            different_years.add(id)

    if len(different_years) > 0:
        print("\nDifferent release years:\n")
        for id in different_years:
            print(f"{sheet_releases[id][0]} - {sheet_releases[id][1]}")
            print_links(id, sheet, sheet_releases)
            print(f"{sheet_releases[id][2]} -> {discogs_releases[id][2]}")
            print()

    sheet_only = sheet_ids - discogs_ids
    if len(sheet_only) > 0:
        print("\nIn sheet but not Discogs:\n")
        for id in sheet_only:
            print(f"{sheet_releases[id][0]} - {sheet_releases[id][1]}")
            print_links(id, sheet, sheet_releases)
            print()

    discogs_only = discogs_ids - sheet_ids
    if len(discogs_only) > 0:
        print("\nIn Discogs but not sheet:\n")
        for id in discogs_only:
            print(f"{discogs_releases[id][0]} - {discogs_releases[id][1]}")
            print(f"https://www.discogs.com/release/{id}")
            print()


def main():
    for sheet, folder in zip(SHEETS.keys(), FOLDERS.keys()):
        compare(sheet, folder)


if __name__ == "__main__":
    main()
