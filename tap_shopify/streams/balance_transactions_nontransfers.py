from datetime import timedelta
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream
from singer import utils, get_logger, metrics

LOGGER = get_logger()

class BalanceTransactionsNontransfers(Stream):
    """Stream class for Shopify balance transactions."""

    name = "balance_transactions"
    data_key = "shopifyPaymentsAccount"
    child_data_key = "balanceTransactions"
    replication_key = "transactionDate"
    # TODO: look how to use a child_replication_key - KEY would be 'type' VALUE would be 'TRANSFER'
    # get_query_params function would be where to change the search

    def get_objects(self):
        """
        Main iterator to yield payout objects.
        """
        sync_start = utils.now().replace(microsecond=0)
        last_updated_at = self.get_bookmark()
        current_bookmark = last_updated_at
        #query = self.remove_fields_from_query(Context.get_unselected_fields(self.name))
        query = self.remove_fields_from_query([])
        LOGGER.info("GraphQL query for stream '%s': %s", self.name, ' '.join(query.split()))

        while last_updated_at < sync_start:
            date_window_end = last_updated_at + timedelta(days=self.date_window_size)
            query_end = min(sync_start, date_window_end)

            has_next_page = True
            cursor = None

            while has_next_page:
                query_params = self.get_query_params(last_updated_at, query_end, cursor)
                with metrics.http_request_timer(self.name):
                    data = self.call_api(query_params, query=query)

                # Process parent objects
                child_data = data.get(self.child_data_key, {})
                for edge in child_data.get("edges", []):
                    obj = self.transform_object(edge.get("node"))
                    replication_value = utils.strptime_to_utc(obj[self.replication_key])
                    current_bookmark = max(current_bookmark, replication_value)
                    yield obj

                page_info =  child_data.get("pageInfo")
                cursor , has_next_page = page_info.get("endCursor"), page_info.get("hasNextPage")

            last_updated_at = query_end
            # Update bookmark to the latest value, but not beyond sync start time
            max_bookmark_value = min(sync_start, current_bookmark)
            self.update_bookmark(utils.strftime(max_bookmark_value))

    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Construct query parameters for GraphQL requests.

        Args:
            updated_at_min (str): Minimum updated_at timestamp.
            updated_at_max (str): Maximum updated_at timestamp.
            cursor (str): Pagination cursor, if any.

        Returns:
            dict: Dictionary of query parameters.
        """
        
        params = {
            "query": f"processed_at:>='{updated_at_min}' AND processed_at:<'{updated_at_max}' AND payout_status:'PAID'",
            "first": self.results_per_page if self.results_per_page <= 150 else 150,
        }

        if cursor:
            params["after"] = cursor
        return params
    
    def get_query(self):
        """
        Returns query for fetching balance transactions.

        Returns:
            str: GraphQL query string.
        """
        return """
        query GetBalanceTransactions($first: Int!, $after: String, $query: String) {
            shopifyPaymentsAccount {
                balanceTransactions(first: $first, after: $after, query: $query, sortKey: PAYOUT_DATE, hideTransfers: true) {
                    edges {
                        cursor
                        node {
                            adjustmentReason
                            adjustmentsOrders {
                                amount {
                                    amount
                                    currencyCode
                                }
                                fees {
                                    amount
                                    currencyCode
                                }
                                link
                                name
                                net {
                                    amount
                                    currencyCode
                                }
                                orderTransactionId
                            }
                            amount {
                                currencyCode
                                amount
                            }
                            associatedOrder {
                                name
                                id
                            }
                            associatedPayout {
                                status
                                id
                            }
                            fee {
                                currencyCode
                                amount
                            }
                            id
                            net {
                                currencyCode
                                amount
                            }
                            sourceOrderTransactionId
                            sourceId
                            sourceType
                            transactionDate
                            type
                        }
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
        }
        """
    
Context.stream_objects["balance_transactions_nontransfers"] = BalanceTransactionsNontransfers