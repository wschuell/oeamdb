from oeamdb import BasgDownloader



def test_download(persistent_tmp_path):
	bdl = BasgDownloader(data_folder=persistent_tmp_path / "basg_dl")
	bdl.download()
