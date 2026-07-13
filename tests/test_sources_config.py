from config.sources import list_source_configs, load_source_config


def test_load_source_config_customers():
    config = load_source_config("customers")
    assert config.source_type == "api"
    assert config.endpoint == "/customers"
    assert config.retry.max_attempts == 4


def test_load_source_config_sales():
    config = load_source_config("sales")
    assert config.source_type == "db"
    assert config.schema_name == "sales"
    assert config.table == "orders"


def test_list_source_configs_finds_all_seven():
    names = {c.name for c in list_source_configs()}
    assert names == {
        "customers",
        "sales",
        "sales_order_lines",
        "sales_products",
        "sales_stores",
        "inventory",
        "suppliers",
    }
