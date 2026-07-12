from config.sources import SourceConfig
from pipelines.ingestion.files.supplier_files import SupplierFileIngestion


def test_extract_reads_all_matching_csv_files(tmp_path):
    file1 = tmp_path / "catalog_a.csv"
    file1.write_text("supplier_id,product_sku,unit_cost\n1,SKU-101,9.99\n")
    file2 = tmp_path / "catalog_b.csv"
    file2.write_text("supplier_id,product_sku,unit_cost\n2,SKU-102,19.99\n")
    (tmp_path / "ignore_me.txt").write_text("not a csv")

    config = SourceConfig(
        name="suppliers", source_type="file", directory=str(tmp_path), file_pattern="*.csv"
    )
    records = list(SupplierFileIngestion(config).extract())

    assert len(records) == 2
    assert records[0]["supplier_id"] == "1"
    assert records[0]["_source_file"] == "catalog_a.csv"
    assert records[1]["_source_file"] == "catalog_b.csv"


def test_extract_returns_empty_when_no_files_match(tmp_path):
    config = SourceConfig(
        name="suppliers", source_type="file", directory=str(tmp_path), file_pattern="*.csv"
    )
    assert list(SupplierFileIngestion(config).extract()) == []
