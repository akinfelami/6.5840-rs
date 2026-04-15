#[cfg(test)]
mod tests {
    use crate::util;

    #[tokio::test]
    async fn test_wc() {
        let tmp = util::mk_out().await;
        let app = "wc";
        let files = util::find_files_pre("../../apps/src/dat", "pg*txt", "").await;
        println!("{}", tmp);
        util::mk_correct_output(&files, app, "mr-wc-correct.txt", &tmp).await;
        util::run_mr(&files, app, 3).await;
        util::merge_output("mr-wc-all.txt", &tmp).await;
        util::run_cmp(
            "mr-wc-all.txt",
            "mr-wc-correct.txt",
            "incorrect combined reduce output: mr-wc-all.txt vs mr-wc-correct.txt",
            &tmp,
        )
        .await;
        assert!(true);
    }
}
