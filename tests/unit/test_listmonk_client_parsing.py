from types import SimpleNamespace

from app.clients.listmonk import _apply_subscriber_update_fields, _extract_list_statuses


def test_extract_list_statuses_prefers_subscription_status() -> None:
    payload = SimpleNamespace(
        lists=[
            {
                "id": 4,
                "status": "active",
                "subscription_status": "confirmed",
            }
        ]
    )

    statuses = _extract_list_statuses(payload)

    assert statuses == {4: "confirmed"}


def test_extract_list_statuses_other_shapes() -> None:
    payload = SimpleNamespace(
        lists=[
            {"id": "5", "status": "active"},
            SimpleNamespace(id="6", status="unconfirmed"),
            SimpleNamespace(id=7, subscription_status="confirmed"),
        ]
    )
    statuses = _extract_list_statuses(payload)
    assert statuses == {5: "active", 6: "unconfirmed", 7: "confirmed"}
    assert _extract_list_statuses(SimpleNamespace(lists=None)) == {}


def test_apply_subscriber_update_fields_for_object_and_dict() -> None:
    obj = SimpleNamespace(email="old@example.com", name="Old", attribs={"k": "v"})
    _apply_subscriber_update_fields(
        subscriber=obj,
        email="new@example.com",
        name="New Name",
        attribs={"user_id": 1},
    )
    assert obj.email == "new@example.com"
    assert obj.name == "New Name"
    assert obj.attribs == {"user_id": 1}

    payload = {"email": "old@example.com", "name": "Old", "attribs": {"k": "v"}}
    _apply_subscriber_update_fields(
        subscriber=payload,
        email="new@example.com",
        name="New Name",
        attribs={"user_id": 1},
    )
    assert payload["email"] == "new@example.com"
    assert payload["name"] == "New Name"
    assert payload["attribs"] == {"user_id": 1}


def test_apply_subscriber_update_fields_immutable_object() -> None:
    class _Immutable:
        def __setattr__(self, name: str, value: object) -> None:
            """
            Prevent attribute assignment by raising RuntimeError.
            
            Raises:
                RuntimeError: Always raised with message "immutable" to indicate the instance is not mutable.
            """
            raise RuntimeError("immutable")

    _apply_subscriber_update_fields(
        subscriber=_Immutable(),
        email="new@example.com",
        name="New Name",
        attribs={"user_id": 1},
    )
