from datetime import timedelta
from tap_shopify.context import Context
from tap_shopify.streams.base import Stream
from singer import utils, get_logger, metrics

LOGGER = get_logger()

class Payouts(Stream):
    """Stream class for Shopify payouts."""

    name = "payouts"
    data_key = "shopifyPaymentsAccount"
    child_data_key = "payouts"
    replication_key = "issuedAt"

    def get_objects(self):
        """
        Main iterator to yield payout objects.
        """
        sync_start = utils.now().replace(microsecond=0)
        last_updated_at = self.get_bookmark() - timedelta(minutes=1)
        current_bookmark = last_updated_at
        query = self.remove_fields_from_query(Context.get_unselected_fields(self.name))
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