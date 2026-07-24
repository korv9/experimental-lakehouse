"""Execute explicit ACON post-actions."""


def run_post_action(spark, action: str, target: str, options: dict) -> None:
    if action == "optimize":
        spark.sql(f"OPTIMIZE {target}")
        return
    if action == "vacuum":
        hours = int(options.get("retention_hours", 168))
        spark.sql(f"VACUUM {target} RETAIN {hours} HOURS")
        return
    raise ValueError(f"unsupported post action: {action}")
