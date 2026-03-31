from tap_shopify.streams.base import Stream
from tap_shopify.streams.payouts import Payouts
from singer import utils, get_logger, metrics

LOGGER = get_logger()

BOOKMARK_KEY = "payout_issuedAt"
MAX_PAYOUTS_PER_RUN = 100


class PayoutDrivenBTStream(Stream):
    """
    Base class for balance transaction streams that sync by payout ID
    instead of date windows. This ensures all BTs for a payout are captured
    even when the payout takes days to reach PAID status.
    """

    name = "balance_transactions"
    data_key = "shopifyPaymentsAccount"
    child_data_key = "balanceTransactions"
    replication_key = "transactionDate"

    def get_objects(self):
        sync_start = utils.now().replace(microsecond=0)
        bookmark_dt = self.get_bookmark_by_name(BOOKMARK_KEY)
        query = self.remove_fields_from_query([])
        LOGGER.info("GraphQL query for stream '%s': %s", self.name, ' '.join(query.split()))

        # Phase 1: Fetch all PAID payout IDs since bookmark
        paid_payouts = Payouts.fetch_paid_payout_ids(bookmark_dt, sync_start)

        if not paid_payouts:
            LOGGER.info("No new PAID payouts found since %s", bookmark_dt)
            return

        LOGGER.info("Found %d PAID payouts (max %d per run)", len(paid_payouts), MAX_PAYOUTS_PER_RUN)

        # Phase 2: For each payout, query BTs by payout legacy ID
        for payout_count, (legacy_id, issued_at) in enumerate(paid_payouts):
            if payout_count >= MAX_PAYOUTS_PER_RUN:
                LOGGER.info("Reached max payouts per run (%d), stopping.", MAX_PAYOUTS_PER_RUN)
                break
            has_next_page = True
            cursor = None

            LOGGER.info("Fetching BTs for payout %s (issuedAt: %s)", legacy_id, issued_at)

            while has_next_page:
                query_params = self.get_query_params_for_payout(legacy_id, cursor)
                with metrics.http_request_timer(self.name):
                    data = self.call_api(query_params, query=query)

                child_data = data.get(self.child_data_key, {})
                for edge in child_data.get("edges", []):
                    obj = self.transform_object(edge.get("node"))
                    yield obj

                page_info = child_data.get("pageInfo")
                cursor, has_next_page = page_info.get("endCursor"), page_info.get("hasNextPage")

            # Bookmark after each fully-completed payout so progress is saved
            # even if the 10k record cap breaks the loop mid-run
            self.update_bookmark(utils.strftime(issued_at), bookmark_key=BOOKMARK_KEY)

    def get_query_params_for_payout(self, payout_legacy_id, cursor=None):
        params = {
            "query": f"payments_transfer_id:{payout_legacy_id} AND payout_status:'PAID'",
            "first": self.results_per_page if self.results_per_page <= 150 else 150,
        }
        if cursor:
            params["after"] = cursor
        return params
