from datetime import timedelta
from tap_shopify.streams.base import Stream
from tap_shopify.context import Context
from singer import utils, get_logger, metrics

LOGGER = get_logger()

BOOKMARK_KEY = "payout_date"
MAX_RECORDS = 10000
DATE_WINDOW_DAYS = 1


class BalanceTransactionsShadow(Stream):
    """
    Shadow stream for validating payout_date windowing approach.
    Instead of iterating individual payouts, queries BTs directly
    using payout_date range windows. Writes to a separate BQ table
    (balance_transactions_shadow_raw) for comparison with production.
    """

    name = "balance_transactions_shadow"
    data_key = "shopifyPaymentsAccount"
    child_data_key = "balanceTransactions"
    replication_key = "transactionDate"

    def get_query(self):
        return """
        query GetBalanceTransactions($first: Int!, $after: String, $query: String) {
            shopifyPaymentsAccount {
                balanceTransactions(first: $first, after: $after, query: $query, sortKey: PAYOUT_DATE) {
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

    def sync(self):
        count = 0
        try:
            for obj in self.get_objects():
                yield obj
                count += 1
                if count >= MAX_RECORDS:
                    bookmark_info = Context.state.get('bookmarks', {}).get(self.name, {})
                    LOGGER.info("Hit %d record cap -- stopping early, will resume on next run. Bookmark state: %s", MAX_RECORDS, bookmark_info)
                    return
        finally:
            status = "HIT CAP -- will resume on next run" if count >= MAX_RECORDS else "completed"
            LOGGER.info("%s: %d records (%s)", self.name, count, status)

    def get_objects(self):
        sync_start = utils.now().replace(microsecond=0)
        bookmark_dt = self.get_bookmark_by_name(BOOKMARK_KEY)
        query = self.remove_fields_from_query([])
        LOGGER.info("GraphQL query for stream '%s': %s", self.name, ' '.join(query.split()))

        window_start = bookmark_dt

        while window_start < sync_start:
            window_end = window_start + timedelta(days=DATE_WINDOW_DAYS)
            if window_end > sync_start:
                window_end = sync_start

            start_date = window_start.strftime("%Y-%m-%d")
            end_date = window_end.strftime("%Y-%m-%d")

            LOGGER.info("Fetching BTs for payout_date window [%s, %s)", start_date, end_date)

            has_next_page = True
            cursor = None
            window_count = 0

            while has_next_page:
                query_params = self.get_query_params_for_window(start_date, end_date, cursor)
                with metrics.http_request_timer(self.name):
                    data = self.call_api(query_params, query=query)

                child_data = data.get(self.child_data_key, {})
                for edge in child_data.get("edges", []):
                    obj = self.transform_object(edge.get("node"))
                    yield obj
                    window_count += 1

                page_info = child_data.get("pageInfo")
                cursor, has_next_page = page_info.get("endCursor"), page_info.get("hasNextPage")

            LOGGER.info("Window [%s, %s): %d BTs", start_date, end_date, window_count)

            # Advance bookmark after each completed window
            self.update_bookmark(utils.strftime(window_end), bookmark_key=BOOKMARK_KEY)
            window_start = window_end

    def get_query_params_for_window(self, start_date, end_date, cursor=None):
        params = {
            "query": f"payout_date:>='{start_date}' AND payout_date:<'{end_date}' AND payout_status:'PAID'",
            "first": self.results_per_page if self.results_per_page <= 150 else 150,
        }
        if cursor:
            params["after"] = cursor
        return params


Context.stream_objects["balance_transactions_shadow"] = BalanceTransactionsShadow
