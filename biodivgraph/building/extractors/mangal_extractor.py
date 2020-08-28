from .http_extractor import HTTPExtractor
import pandas as pd

# Obsolete : Mangal is already integrated into GloBi


class MangalExtractor(HTTPExtractor):
    def __init__(self, config):
        HTTPExtractor.__init__(self)
        self.config = config
        self.tax_info_colnames = [
            "id",
            "name",
            "ncbi",
            "tsn",
            "eol",
            "bold",
            "gbif",
            "col",
            "rank",
            "created_at",
            "updated_at",
        ]

    def fetch_dataset(self):
        # Get interaction data
        page = 0
        while True:
            inter_json = self.get_json_results(
                url="https://mangal.io/api/v2/interaction",
                params={"page": page, "count": 1000},
            )
            page += 1
            inter_df = pd.DataFrame.from_records(inter_json)
            print(page, inter_df.shape)

        inter_df = inter_df.head(3)

        # Get node data
        node_from_df = pd.DataFrame(
            columns=["source_taxon_" + colname for colname in self.tax_info_colnames]
        )
        node_to_df = pd.DataFrame(
            columns=["target_taxon_" + colname for colname in self.tax_info_colnames]
        )

        n = 0
        for index, row in inter_df.iterrows():
            tax_info_df = self.get_taxonomic_info(row["node_from"])
            tax_info_df.columns = node_from_df.columns
            node_from_df = node_from_df.append(tax_info_df, ignore_index=True)

            tax_info_df = self.get_taxonomic_info(row["node_to"])
            tax_info_df.columns = node_to_df.columns
            node_to_df = node_to_df.append(tax_info_df, ignore_index=True)

            n += 1
            if n == 3:
                break

        result = pd.concat([inter_df, node_from_df, node_to_df], axis=1)
        return result

    def get_taxonomic_info(self, node_id):
        node_json = self.get_json_results(
            url="https://mangal.io/api/v2/node/{}".format(node_id)
        )
        tax_info_dict = node_json["taxonomy"]
        if not tax_info_dict:
            tax_info_dict = {colname: None for colname in self.tax_info_colnames}
            tax_info_dict["name"] = node_json["original_name"]
        return pd.DataFrame.from_dict([tax_info_dict])


def main():
    extractor = MangalExtractor(config)
    extractor.fetch_dataset()


if __name__ == "__main__":
    main()
