import yaml
from pydantic import BaseModel


class ProjectConfig(BaseModel):
    """Represent project configuration parameters loaded from YAML."""

    catalog: str
    schema: str
    volume: str
    url: str

    @classmethod
    def from_yaml(cls, config_path: str) -> "ProjectConfig":
        """Load and parse configuration settings from a YAML file.

        :param config_path: Path to the YAML configuration file
        :return: ProjectConfig instance initialized with parsed configuration
        """
        with open(config_path) as f:
            config_dict = yaml.safe_load(f)
            return cls(**config_dict)
