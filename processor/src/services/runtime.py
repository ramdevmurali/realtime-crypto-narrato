from ..config import settings, get_thresholds, get_windows


def log_startup_config(log) -> None:
    windows = get_windows()
    log.info(
        "processor_config",
        extra={
            "config": settings.safe_dict(),
            "windows": {k: int(v.total_seconds()) for k, v in windows.items()},
            "window_labels": list(windows.keys()),
            "thresholds": get_thresholds(),
            "topics": {
                "price_topic": settings.price_topic,
                "news_topic": settings.news_topic,
                "news_enriched_topic": settings.news_enriched_topic,
                "alerts_topic": settings.alerts_topic,
                "summaries_topic": settings.summaries_topic,
                "summaries_dlq_topic": settings.summaries_dlq_topic,
                "price_dlq_topic": settings.price_dlq_topic,
                "news_dlq_topic": settings.news_dlq_topic,
            },
            "policies": {
                "late_price_tolerance_sec": settings.late_price_tolerance_sec,
                "anomaly_cooldown_sec": settings.anomaly_cooldown_sec,
                "headline_max_age_sec": settings.headline_max_age_sec,
                "rss_seen_ttl_sec": settings.rss_seen_ttl_sec,
                "rss_seen_max": settings.rss_seen_max,
                "window_max_gap_factor": settings.window_max_gap_factor,
                "vol_resample_sec": settings.vol_resample_sec,
                "sentiment_provider": settings.sentiment_provider,
                "sentiment_batch_size": settings.sentiment_batch_size,
                "sentiment_max_latency_ms": settings.sentiment_max_latency_ms,
                "retry_max_attempts": settings.retry_max_attempts,
                "retry_backoff_base_sec": settings.retry_backoff_base_sec,
                "retry_backoff_cap_sec": settings.retry_backoff_cap_sec,
            },
        },
    )
