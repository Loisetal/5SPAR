from mastodon import Mastodon

mastodon = Mastodon(
    access_token="SkRBTtrjicN1-imUK-dTgLKm5b7j5N1RXaHugvvc5m4",
    api_base_url="https://mastodon.social"
)

timeline = mastodon.timeline_public(limit=3)
for toot in timeline:
    print(toot["account"]["username"], ":", toot["content"])
