import importlib.util
import os

USER_CONFIG_FILE = os.getenv("TPCH_RUNNER_CONFIG", "runner_config.py")


class Config:
    precision = 0.0001
    app_root = "~/data/tpch_runner"
    data_dir = "~/data/tpch_runner/data"
    result_dir = "~/data/tpch_runner/results"

    @classmethod
    def load_user_config(cls, USER_CONFIG_FILE):
        if os.path.exists(USER_CONFIG_FILE):
            spec = importlib.util.spec_from_file_location(
                "runner_config", USER_CONFIG_FILE
            )
            if not spec:
                raise ValueError("Invalid specification")
            user_config = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(user_config)  # type: ignore

            for k, v in vars(user_config).items():
                if not k.startswith("__") and hasattr(cls, k):
                    setattr(cls, k, v)
