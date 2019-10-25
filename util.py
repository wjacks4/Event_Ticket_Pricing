from dataclasses import dataclass

@dataclass
class Credential:

    Spotify_client_ID: str
    Spotify_client_secret: str
    uses: int