# Real Teyca webhook bodies (teyca-sync-iil.3)

These are real `pass` objects pulled live from `GET /v1/{token}/passes/userid/{user_id}`
during the teyca-sync-iil incident investigation (2026-09-04), with PII scrubbed
(`fio`, `first_name`, `last_name`, `email`, `phone`, `telegram_chat_id`, `link`) and
`user_id` replaced with an out-of-range placeholder. Everything else — field shapes,
numeric-looking strings (`"summ": "130402.0"`), null holes in `tags`, unset-but-present
keys — is untouched, because that's exactly the kind of thing that broke
`app/schemas/webhook.py` before (teyca-sync-iil.1, teyca-sync-iil.2).

Per `docs/teyca-api.md`, the `pass` object has the same shape for `CREATE` and
`UPDATE` — only `type` differs. `create.json` and `update_tags_null_hole.json`
are the same captured card wrapped under different `type`, which is the closest
thing to a real `CREATE` body without lucky timing on the webhook endpoint itself.
`delete.json` is deliberately minimal: `docs/teyca-api.md` and
`app/consumers/delete_user.py` agree that a `DELETE` payload only needs
`pass.user_id` to route the event; there's no evidence Teyca sends the full card
on delete.

- `create.json` — `CREATE`, ordinary card.
- `update_tags_null_hole.json` — `UPDATE`, `"tags": [null]` (the exact shape of
  the 373 dropped events from user 4327198's card, teyca-sync-iil.1).
- `update_tags_multiple_with_hole.json` — `UPDATE`, `"tags": [1551, null]` (the
  `pass.tags.1` variant, 146 dropped events, user 5411336's card).
- `delete.json` — `DELETE`, minimal.

Used by `tests/unit/test_webhook_fixtures.py`.
