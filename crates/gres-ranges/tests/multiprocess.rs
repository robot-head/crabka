mod harness;

use harness::TwoComputeHarness;

#[tokio::test]
async fn two_range_computes_accept_forwarded_dml_on_hosted_ranges() {
    let computes = TwoComputeHarness::start("tenant_multiprocess");
    computes.create_table_on_all_computes("t150").await;
    computes.create_table_on_all_computes("t250").await;

    computes.forwarded_insert(150, 10).await;
    computes.forwarded_insert(250, 20).await;

    assert_eq!(computes.count_rows(150).await, 1);
    assert_eq!(computes.count_rows(250).await, 1);
}
