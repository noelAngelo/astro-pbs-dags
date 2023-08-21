# import the hook to inherit from
from airflow.hooks.base import BaseHook
import pandas as pd


# define the class inheriting from an existing hook class
class PharmaceuticalBenefitsSchemeHook(BaseHook):
    """
    Interact with Pharmaceutical Benefits Scheme.
    :param my_conn_id: ID of the connection to Pharmaceutical Benefits Scheme
    """

    # provide the name of the parameter which receives the connection id
    conn_name_attr = "conn_id"
    # provide a default connection id
    default_conn_name = "pbs_default"
    # provide the connection type
    conn_type = "general"
    # provide the name of the hook
    hook_name = "PharmaceuticalBenefitsSchemeHook"

    # define the .__init__() method that runs when the DAG is parsed
    def __init__(
        self, conn_id: str = default_conn_name, *args, **kwargs
    ) -> None:
        # initialize the parent hook
        super().__init__(*args, **kwargs)
        # assign class variables
        self.conn_id = conn_id
        # (optional) call the '.get_conn()' method upon initialization
        self.get_conn()

    def get_conn(self):
        """Function that initiates a new connection to your external tool."""
        # retrieve the passed connection id
        conn_id = getattr(self, self.conn_name_attr)
        # get the connection object from the Airflow connection
        conn = self.get_connection(conn_id)

        return conn

    # add additional methods to define interactions with your external tool
    def get_date_of_supply(self, endpoint: str):
        """Function that retrieves the date of supply data from PBS"""
        conn = self.get_conn
        host = conn.host.rstrip('/')
        data = pd.read_csv(f'{host}{endpoint}')
        return data.to_dict(orient='records')