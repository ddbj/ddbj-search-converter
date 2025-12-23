from ddbj_search_converter.config import get_config
from ddbj_search_converter.dblink.db import init_dblink_db
from ddbj_search_converter.logging.logger import init_logger, log


def main() -> None:
    config = get_config()
    init_logger(
        run_name="init_dblink_db",
        config=config,
    )
    log(
        event="start",
        message="initializing DBLink relation database",
        extra={"config": config.model_dump()}
    )

    try:
        init_dblink_db(config)
        log(event="end", message="DBLink relation database initialized successfully")
    except Exception as e:
        log(event="failed", error=e)
        raise e


if __name__ == "__main__":
    main()
