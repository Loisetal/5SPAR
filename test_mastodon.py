from mastodon import Mastodon
import os

mastodon = Mastodon(
    access_token=os.getenv("ACCESS_TOKEN"),
    api_base_url=os.getenv("INSTANCE_URL")
)

timeline = mastodon.timeline_public(limit=3)
for toot in timeline:
    print(toot["account"]["username"], ":", toot["content"])
