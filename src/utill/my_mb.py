from .my_const import HttpMethod
from .my_env import MB_FILENAME
from loguru import logger
import json


class MB:
    def __init__(self, config_source: str = MB_FILENAME) -> None:
        config = json.loads(open(config_source, "r").read())

        self.base_url = config["base_url"]
        self.api_key = config["api_key"]

        logger.info(f"✅ Initialized {self.base_url}")

    # region Utility

    def http_request(self, method, url, **kwargs):
        url = f"{self.base_url}/{url.lstrip('/')}"
        kwargs.setdefault("headers", {"x-api-key": self.api_key})
        return super().http_request(method, url, **kwargs)

    def decode_collection_location_to_group(self, location: str):
        group_names = []
        for collection_id in location.strip("/").split("/"):
            # if collection_id not in self.known_collections_by_id:
            #     collection = self.get_collection(collection_id)
            #     self.known_collections_by_id[collection_id] = collection

            # group_names.append(self.known_collections_by_id[collection_id]['name'])

            collection = self.get_collection(collection_id)
            group_names.append(collection["name"])

        return " > ".join(group_names)

    @staticmethod
    def translate_user_group_ids(user: dict) -> set:
        return set(user["group_ids"]) - {1}  # Exclude 'All Users' group

    # endregion

    # region User

    def get_all_users(self, all=False) -> list[dict]:
        params = {}
        if all:
            params["status"] = "all"
        return self.http_request(
            HttpMethod.GET,
            "api/user",
            params=params,
        ).json()["data"]

    def get_user(
        self,
        user_id: int,
    ) -> dict:
        return self.http_request(HttpMethod.GET, f"api/user/{user_id}").json()

    def create_user(
        self, first_name: str, last_name: str, email: str, group_ids: list[int] = [1]
    ) -> dict:
        new_user = self.http_request(
            HttpMethod.POST,
            "api/user",
            json={
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "user_group_memberships": [{"id": group_id} for group_id in group_ids],
            },
        ).json()
        logger.debug(f"✅ User [{new_user['id']}] {email} created")
        return new_user

    def disable_user(self, id: str):
        self.http_request(HttpMethod.DELETE, f"api/user/{id}")
        logger.debug(f"✅ User {id} disabled")

    def enable_user(self, id: str):
        self.http_request(HttpMethod.PUT, f"api/user/{id}/reactivate")
        logger.debug(f"✅ User {id} enabled")

    def reset_password(self, email: str):
        self.http_request(
            HttpMethod.POST, "api/session/forgot_password", json={"email": email}
        )
        logger.debug(f"✅ User {email} password has been reset")

    # endregion

    # region Group

    def get_all_groups(self) -> list[dict]:
        return self.http_request(HttpMethod.GET, "api/permissions/group").json()

    def get_group(self, group_id: int) -> dict:
        return self.http_request(
            HttpMethod.GET, f"api/permissions/group/{group_id}"
        ).json()

    def create_group(self, group_name: str):
        self.http_request(
            HttpMethod.POST,
            "api/permissions/group",
            json={
                "name": group_name,
            },
        )
        logger.debug(f"✅ Group {group_name} created")

    def delete_group(self, id: str):
        self.http_request(
            HttpMethod.DELETE,
            f"api/permissions/group/{id}",
        )
        logger.debug(f"✅ Group {id} deleted")

    # endregion

    # region Question / card

    def get_question(self, id: int) -> dict:
        return self.http_request(HttpMethod.GET, f"api/card/{id}").json()

    def change_question_connection(self, id: int, connection_id: int) -> None:
        dataset_query = self.get_question(id)["dataset_query"]
        if dataset_query["database"] == connection_id:
            logger.warning(f"⚠️ Question {id} already using connection {connection_id}")
            return
        dataset_query["database"] = connection_id
        self.http_request(
            HttpMethod.PUT,
            f"api/card/{id}",
            json={"dataset_query": dataset_query},
        )
        logger.debug(f"✅ Question {id} connection changed to {connection_id}")

    def archive_question(self, id: int) -> None:
        self.http_request(HttpMethod.PUT, f"api/card/{id}", json={"archived": True})
        logger.debug(f"✅ Question {id} archived")

    # endregion

    # region Dashboard

    def get_dashboard(self, id: int) -> None:
        return self.http_request(HttpMethod.GET, f"api/dashboard/{id}").json()

    # endregion

    # region Collection

    # def get_all_collections(self, collection_id: int) -> dict:
    #     logger.debug("🕐 Initialize collection data")
    #     response_json = [
    #         x for x in self.http_request(HttpMethod.GET, "api/collection").json()[1:]
    #     ]  # Exclude root collection
    #     self.dict__collection_id__collection_name = {
    #         x["id"]: x["name"] for x in response_json
    #     }
    #     self.dict__collection_id__collection = {
    #         x["id"]: {
    #             **x,
    #             "group_name": (
    #                 " > ".join(
    #                     [
    #                         self.decode_collection_location_to_group(
    #                             self.dict__collection_id__collection_name,
    #                             x["location"],
    #                         ),
    #                         x["name"],
    #                     ]
    #                 )
    #                 if x["location"] != "/"
    #                 else x["name"]
    #             ),
    #         }
    #         for x in response_json
    #         if x["personal_owner_id"] is None
    #     }

    #     if collection_id in self.dict__collection_id__collection:
    #         return self.dict__collection_id__collection[collection_id]
    #     else:
    #         return self.http_request(
    #             HttpMethod.GET, f"api/collection/{collection_id}"
    #         ).json()

    def get_collection(self, id: int) -> dict:
        return self.http_request(HttpMethod.GET, f"api/collection/{id}").json()

    # endregion

    # region Permission

    def grant_user_to_group(self, user_id: int, group_id: int) -> None:
        self.http_request(
            HttpMethod.POST,
            "api/permissions/membership",
            json={
                "group_id": group_id,
                "user_id": user_id,
                "is_group_manager": False,
            },
        )
        logger.debug(f"✅ Granted user {user_id} to group {group_id}")

    def grant_group_to_collection(self, group_id: int, collection_id: int):
        group_id = str(group_id)
        collection_id = str(collection_id)

        # Get latest revision
        graph = self.http_request(HttpMethod.GET, "api/collection/graph").json()
        logger.debug(f'Latest revision: {graph["revision"]}')

        # Update revision grpah
        self.http_request(
            HttpMethod.PUT,
            "api/collection/graph",
            json={
                "revision": graph["revision"],
                "groups": {group_id: {collection_id: "read"}},
            },
        )
        logger.debug(f"✅ Granted group {group_id} to collection {collection_id}")

    def mirror_permission(self, src_user_id: str, dst_user_id: str) -> None:
        src_user = self.get_user(src_user_id)
        dst_user = self.get_user(dst_user_id)

        src_user_group_ids = src_user["group_ids"]
        dst_user_group_ids = dst_user["group_ids"]
        group_ids_to_grant = list(set(src_user_group_ids) - set(dst_user_group_ids))
        for group_id_to_grant in group_ids_to_grant:
            self.grant_user_to_group(dst_user_id, group_id_to_grant)

    # endregion

    # region Other utilities

    @staticmethod
    def get_object_info_from_url(url: str) -> tuple[str, int]:
        # Get information for this object
        logger.info(f"Getting Metabase object information from {url}")
        url = (
            str(url).removeprefix("http://").removeprefix("https://")
        )  # https://somesite/question/1234-xxx-yyy
        _, object_type, object_id = url.split(
            "/", 3
        )  # somesite/question/1234-xxx-yyy
        object_id = int(object_id.split("-", 1)[0])  # 1234-xxx-yyy

        return object_type, object_id

    # endregion

    # region Final function

    def grant_metabase_access(
        self,
        metabase_url: str,
        emails: list[str],
        create_user_if_not_exists: bool = False,
    ):
        all_users_by_email = {
            user["email"]: user for user in self.get_all_users(all=True)
        }
        all_groups_by_name = {x["name"]: x for x in self.get_all_groups()}

        # Get information for this object
        logger.info("Getting Metabase object information")
        object_type, object_id = self.get_object_info_from_url(metabase_url)
        collection_id: int | None = None
        collection_location: str | None = None
        match (object_type):
            case "question":
                question = self.get_question(object_id)
                collection_id = int(question["collection"]["id"])
                collection_location = question["collection"]["location"] + str(
                    question["collection"]["id"]
                )
            case "dashboard":
                dashboard = self.get_dashboard(object_id)
                collection_id = int(dashboard["collection"]["id"])
                collection_location = dashboard["collection"]["location"] + str(
                    dashboard["collection"]["id"]
                )
            case "collection":
                collection = self.get_collection(object_id)
                collection_id = object_id
                collection_location = collection["location"] + str(
                    collection["collection"]["id"]
                )
            case _:
                raise ValueError(
                    f"Unknown object type {object_type} from {metabase_url}"
                )
        logger.info(
            f'Object found: type "{object_type}", ID {object_id}, collection ID {collection_id}'
        )

        # Get group info that this collection should be granted to
        logger.info(f"Getting group information for the object: {collection_location}")
        group_name = self.decode_collection_location_to_group(collection_location)
        if group_name not in all_groups_by_name:
            # If group not exists, create it and immediately grant readonly access to the collectiond
            self.create_group(group_name)
            all_groups_by_name = {x["name"]: x for x in self.get_all_groups()}
            group_id = int(all_groups_by_name[group_name]["id"])
            self.grant_group_to_collection(group_id, collection_id)
        else:
            group_id = int(all_groups_by_name[group_name]["id"])
        logger.info(f"Group found: [{group_id}] {group_name}")

        # Get user informations, create if not exists
        logger.info(f"Getting information from {len(emails)} users")
        users = set()
        created_users = 0
        not_found_emails = []
        for email in emails:
            if email not in all_users_by_email:
                if create_user_if_not_exists:
                    logger.info(f"Creating user {email}")
                    email_name, email_domain = email.split("@", 1)
                    self.create_user(
                        first_name=email_name,
                        last_name=email_domain,
                        email=email,
                        group_ids=[1],  # Add to 'All Users' group
                    )
                    # all_users_by_email = {
                    #     user["email"]: user for user in self.get_all_users(all=True)
                    # }
                    created_users += 1
                else:
                    not_found_emails.append(email)
        if not_found_emails:
            raise ValueError(f"Users not found: {', '.join(not_found_emails)}")

        # Re-fetch all users if there are new users created
        if created_users:
            logger.info("Users created, re-fetching all users")
            all_users_by_email = {
                user["email"]: user for user in self.get_all_users(all=True)
            }

        # Grant access
        logger.info(
            f"Granting access to group [{group_id}] {group_name} for {len(emails)} users"
        )
        for email in emails:
            user = all_users_by_email[email]
            if (
                not user["is_active"]
            ) and create_user_if_not_exists:  # Reactivate user if disabled
                logger.info(f"Reactivating user {user['id']}")
                self.enable_user(user["id"])

            user_id = int(user["id"])
            user_email = user["email"]
            if group_id in user["group_ids"]:
                # Skip if user already in the group because it will cause 500 error on Metabase later (it tries to insert the permissions to its DB and got duplicate key error)
                logger.info(f"User {user_id} already in group {group_id}, skipping")
                continue
            users.add((user_id, user_email))
        logger.info(
            f"Users to be granted: {', '.join([f'[{user_id}] {user_email}' for user_id, user_email in users])}"
        )

        # Assign all user to the group
        for user_id, user_email in users:
            logger.info(f"Assigning user {user_id} to group {group_id}")
            self.grant_user_to_group(user_id, group_id)
        logger.info("All users assigned to the group")

    # endregion
