from jobs.crontab.svn_checkout import checkout

SVN_CRED_ENV = "PYTOOLS_SVN_PASSWORD"


if __name__ == "__main__":
    checkout(
        url_env="PYTOOLS_UPSTREAM_SVN_URL",
        directory_env="PYTOOLS_UPSTREAM_WORKSPACE",
        default_url="svn://svn.example.invalid/upstream/trunk",
        default_directory="runtime/workspaces/upstream",
        username_env="PYTOOLS_SVN_USERNAME",
        password_env=SVN_CRED_ENV,
    )
