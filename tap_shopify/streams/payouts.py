from datetime import timedelta
from singer import metrics, utils
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

class Payouts(Stream):
    """Stream class for Shopify payouts."""

    name = "payouts"
    data_key = "payouts"
    replication_key = "issuedAt"

    def get_query_params(self, issued_at_min, issued_at_max, cursor=None):
        """
        Construct query parameters for GraphQL requests.

        Args:
            issued_at_min (str): Minimum issued_at timestamp.
            issued_at_max (str): Maximum issued_at timestamp.
            cursor (str): Pagination cursor, if any.

        Returns:
            dict: Dictionary of query parameters.
        """
        rkey = self.camel_to_snake(self.replication_key)

        # TODO: In future once the dynamic query generation logic is setup remove the below
        # condition for the orders stream. As by default we will ask the customers to select
        # few fields or reduce the page size
        params = {
            "query": f"{rkey}:>='{issued_at_min}' AND {rkey}:<'{issued_at_max}'",
            "first": self.results_per_page if self.results_per_page <= 150 else 150,
        }

        if cursor:
            params["after"] = cursor
        return params
    
    def get_query(self):
        """
        Returns query for fetching payouts.

        Returns:
            str: GraphQL query string.
        """
        return """
            query GetPayouts($first: Int!, $after: String, $query: String) {
                shopifyPaymentsAccount {
                    payouts(first: $first, after: $after, query: $query, sortKey: ISSUED_AT) {
                        edges {
                            node {
                                id
                                status
                                transactionType
                                issuedAt
                                net {
                                    amount
                                    currencyCode
                                }
                                summary {
                                    adjustmentsFee {
                                        amount
                                        currencyCode
                                    }
                                    retriedPayoutsGross {
                                        amount
                                        currencyCode
                                    }
                                    retriedPayoutsFee {
                                        amount
                                        currencyCode
                                    }
                                    reservedFundsGross {
                                        amount
                                        currencyCode
                                    }
                                    reservedFundsFee {
                                        amount
                                        currencyCode
                                    }
                                    refundsFeeGross {
                                        amount
                                        currencyCode
                                    }
                                    refundsFee {
                                        amount
                                        currencyCode
                                    }
                                    chargesGross {
                                        amount
                                        currencyCode
                                    }
                                    chargesFee {
                                        amount
                                        currencyCode
                                    }
                                    advanceGross {
                                        amount
                                        currencyCode
                                    }
                                    advanceFees {
                                        amount
                                        currencyCode
                                    }
                                    adjustmentsGross {
                                        amount
                                        currencyCode
                                    }
                                }
                                gross {
                                    amount
                                    currencyCode
                                }
                            }
                        }
                        pageInfo {
                            endCursor
                            hasNextPage
                        }
                    }
                }
            }
        """
    
Context.stream_objects["payouts"] = Payouts