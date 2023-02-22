"""CSV/XLSX Export Module
"""
import dataclasses
import pandas as pd

@dataclasses.dataclass
class CSVData:
    """Class for export data
    """
    def __init__(self, df_warm: pd.DataFrame, df_warm_sum: pd.DataFrame, df_replica: pd.DataFrame,
                 df_replica_sum: pd.DataFrame, df_view: pd.DataFrame, df_view_sum: pd.DataFrame,
                 df_vault: pd.DataFrame, df_vault_sum: pd.DataFrame):
        self.df_warm = df_warm
        self.df_warm_sum = df_warm_sum
        self.df_replica = df_replica
        self.df_replica_sum = df_replica_sum
        self.df_view = df_view
        self.df_view_sum = df_view_sum
        self.df_vault = df_vault
        self.df_vault_sum = df_vault_sum

    def save_as_csv(self, reportname) -> None:
        """Output to excel

        Args:
            reportname (str): Name of the report
        """
        with pd.ExcelWriter(reportname) as writer: # pylint: disable=abstract-class-instantiated
            self.df_warm.to_excel(writer, sheet_name="Warm Granular", index=False)
            self.df_warm_sum.to_excel(writer, sheet_name="Warm Sum", index=False)
            self.df_replica.to_excel(writer, sheet_name="Replica Granular", index=False)
            self.df_replica_sum.to_excel(writer, sheet_name="Replica Sum", index=False)
            self.df_view.to_excel(writer, sheet_name="Cold Granular", index=False)
            self.df_view_sum.to_excel(writer, sheet_name="Cold Sum", index=False)
            self.df_vault.to_excel(writer, sheet_name="Frozen Granular", index=False)
            self.df_vault_sum.to_excel(writer, sheet_name="Frozen Sum", index=False)
