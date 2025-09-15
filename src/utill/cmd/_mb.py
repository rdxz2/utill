
def _jl_grant(emails: list[str], url: str, create_user_if_not_exists: bool = False):
    from ..my_mb import MB
    from loguru import logger

    mb = MB()
    all_users_by_email = {
        user["email"]: user for user in mb.get_all_users(all=True)
    }
    all_groups_by_name = {x["name"]: x for x in mb.get_all_groups()}

    # Get information for this object
    logger.info("Getting Metabase object information")
    object_type, object_id = mb.get_object_info_from_url(url)
    collection_id: int | None = None
    collection_location: str | None = None
    match (object_type):
        case "question":
            question = mb.get_question(object_id)
            collection_id = int(question["collection"]["id"])
            collection_location = question["collection"]["location"] + str(
                question["collection"]["id"]
            )
        case "dashboard":
            dashboard = mb.get_dashboard(object_id)
            collection_id = int(dashboard["collection"]["id"])
            collection_location = dashboard["collection"]["location"] + str(
                dashboard["collection"]["id"]
            )
        case "collection":
            collection = mb.get_collection(object_id)
            collection_id = object_id
            collection_location = collection["location"] + str(
                collection["collection"]["id"]
            )
        case _:
            raise ValueError(
                f"Unknown object type {object_type} from {url}"
            )
    logger.info(
        f'Object found: type "{object_type}", ID {object_id}, collection ID {collection_id}'
    )

    # Get group info that this collection should be granted to
    logger.info(f"Getting group information for the object: {collection_location}")
    group_name = mb.decode_collection_location_to_group(collection_location)
    if group_name not in all_groups_by_name:
        # If group not exists, create it and immediately grant readonly access to the collectiond
        mb.create_group(group_name)
        all_groups_by_name = {x["name"]: x for x in mb.get_all_groups()}
        group_id = int(all_groups_by_name[group_name]["id"])
        mb.grant_group_to_collection(group_id, collection_id)
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
                mb.create_user(
                    first_name=email_name,
                    last_name=email_domain,
                    email=email,
                    group_ids=[1],  # Add to 'All Users' group
                )
                # all_users_by_email = {
                #     user["email"]: user for user in mb.get_all_users(all=True)
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
            user["email"]: user for user in mb.get_all_users(all=True)
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
            mb.enable_user(user["id"])

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
        mb.grant_user_to_group(user_id, group_id)
    logger.info("All users assigned to the group")


def _copy_permissions(src_email: str, dst_emails: list[str]):
    from ..my_mb import MB

    mb = MB()
    for dst_email in dst_emails:
        mb.mirror_permission(src_email, dst_email)


def _reset_password(emails: list[str]):
    from ..my_mb import MB

    mb = MB()
    for email in emails:
        mb.reset_password(email)


def _disable_user(emails: list[str]):
    from ..my_mb import MB

    mb = MB()
    for email in emails:
        mb.disable_user(email)
