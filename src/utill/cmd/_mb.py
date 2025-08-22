def _grant(emails: list[str], urls: list[str]):
    from ..my_mb import MB

    mb = MB()
    mb.make_sure_all_email_exists(emails)

    question_urls = []
    collection_urls = []
    dashboard_urls = []
    for url in urls:
        if not url.startswith(mb.base_url):
            raise ValueError(f'URL must be a Metabase URL: {url}')

        url_stripped = url.removeprefix(mb.base_url)
        if url_stripped.startswith('/collection/'):
            collection_urls.append(url)
        elif url_stripped.startswith('/question/'):
            question_urls.append(url)
        elif url_stripped.startswith('/dashboard/'):
            dashboard_urls.append(url)
        else:
            raise ValueError(f'URL is neither a collection/question/dashboard: {url} --> {url_stripped}')

    for email in emails:
        for question_url in question_urls:
            mb.grant_user_email_to_question_by_url(email, question_url)
        for collection_url in collection_urls:
            mb.grant_user_email_to_collection_by_url(email, collection_url)
        for dashboard_url in dashboard_urls:
            mb.grant_user_email_to_dashboard_by_url(email, dashboard_url)


def _copy_permissions(src_email: str, dst_emails: list[str]):
    from ..my_mb import MB

    mb = MB()
    mb.make_sure_all_email_exists([src_email] + dst_emails)

    for dst_email in dst_emails:
        mb.mirror_user_permission_by_email(src_email, dst_email)


def _reset_password(emails: list[str]):
    from ..my_mb import MB

    mb = MB()
    mb.make_sure_all_email_exists(emails)

    for email in emails:
        mb.reset_password_by_email(email)


def _deactivate_user(emails: list[str]):
    from ..my_mb import MB

    mb = MB()
    mb.make_sure_all_email_exists(emails)

    for email in emails:
        mb.deactivate_user_by_email(email)
