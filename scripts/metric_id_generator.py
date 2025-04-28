# metric_id_generator.py

"""
Scritp to assign metric_id to ESG metrics

"""

import pandas as pd

from scripts.utils.config import get_config

config = get_config("metric_id_generator", auto_date=False, fixed_date="202505")
logger = config["logger"]

base_dir_path = config["ESG_METRICS_MAP_DIR"]
input_file_path = base_dir_path / "esg_metrics_temp.csv"
output_file_path = base_dir_path / "esg_dim_metrics.csv"

# Mapping dictionaries
provider_prefixes = {
    "Clarity.ai": "clar",
    "NASDAQ": "nasd",
    "Sustainalytics": "sust",
    "Santander Group": "sant",
}

category_codes = {
    "Flags": "00",
    "ESG Score Rating": "01",
    "Controversy Metrics": "02",
    "Expousure Metrics": "03",
    "Group Policies": "04",
}

data_type_codes = {
    "boolean": "00",
    "string": "01",
    "int": "02",
    "float": "03",
    "long": "04",
    "double": "05",
    "decimal": "06",
    "date": "07",
    "timestamp": "08",
    "binary": "09",
    "list": "10",
    "map": "11",
}


# Function to determine theme code
def get_theme_code(metric_name):
    themes = {
        "animal_welfare": "001",
        "animal_testing": "001",
        "armament": "002",
        "weapons": "002",
        "nuclear_weapons": "003",
        "nuclear_energy": "004",
        "chemic_biolog_weapons": "005",
        "cluster_bombs": "006",
        "antip_landmines": "007",
        "depleted_uranium": "008",
        "white_phosporus": "009",
        "small_arms": "010",
        "fossil_fuels": "011",
        "coal_mining": "012",
        "thermal_coal": "012",
        "coal_power_gen": "013",
        "oil_fuels": "014",
        "oil_sands": "015",
        "artic_oil": "016",
        "gas_fuels": "017",
        "shale_energy": "018",
        "agrochemical_products": "019",
        "gmo_products": "020",
        "gmo_research": "020",
        "meat_products": "021",
        "pork_products": "022",
        "palm_oil": "023",
        "alcohol": "024",
        "alchol": "024",
        "tobacco": "025",
        "cannabis": "026",
        "gambling": "027",
        "pornography": "028",
        "fur_exotic_leather": "029",
        "abortifacents": "030",
        "contraceptives": "030",
        "embryonic_stem_cell_research": "031",
        "stem_cell_research": "032",
        "predatory_lending": "033",
        "carbon": "034",
        "ghg_emissions": "034",
        "emissions_effluents": "035",
        "water_use": "035",
        "land_use_biodiversity": "035",
        "environmental_impact_of_products": "035",
        "community_relations": "036",
        "labour_relations": "036",
        "human_rights": "036",
        "basic_services": "036",
        "corporate_governance": "037",
        "business_ethics": "037",
        "lobbying_public_policy": "037",
        "bribery_corruption": "038",
        "accounting_taxation": "039",
        "intellectual_property": "039",
        "occupational_health_and_safety": "040",
        "quality_and_safety": "040",
        "data_privacy_security": "041",
        "anticompetitive_practices": "042",
        "marketing_practices": "043",
        "media_ethics": "043",
        "sanctions": "044",
        "bond": "045",
        "nasdaq_bond_flag": "045",
        "esg_score": "046",
        "esg_rating": "046",
        "esg_score_relevance": "046",
        "sustainability_rating": "047",
        "global_sustainability_rating": "047",
        "climate_policy": "048",
        "defense_policy": "048",
        "group_policy": "048",
        "highest_controversy_level_answer_category": "049",
        "global_compact_compliance": "050",
    }

    for theme, code in themes.items():
        if theme in metric_name:
            logger.info("code found!")
            return code
    return "999"  # default catch-all code


# Function to determine the final digit
def get_final_digit(category, name, provider):
    if category == "Expousure Metrics":
        if name.endswith("_prod"):
            return "1"
        elif name.endswith("_part"):
            return "0"
    if category == "Controversy Metrics":
        if "critical" in name or "high" in name:
            return "3"
        else:
            return "5"
    if category == "Flags":
        return "7"
    if provider == "Nasdaq" and "bond" in name:
        return "7"
    if (
        provider == "Sustainalytics"
        and "overall_global_compact_compliance_status" in name
    ):
        return "7"
    return "5"


# Generate metric_id
def generate_metric_id(row):
    prefix = provider_prefixes.get(row["data_provider"], "unkn")
    category = category_codes.get(row["metric_category"], "99")
    data_type = data_type_codes.get(row["metric_type"], "99")
    theme_code = get_theme_code(row["metric_name"])
    final_digit = get_final_digit(
        row["metric_category"], row["metric_name"], row["data_provider"]
    )
    return f"{prefix}{category}{data_type}{theme_code}{final_digit}"


def main():
    # Load the CSV file into DataFrame
    df = pd.read_csv(input_file_path)

    # Apply function to DataFrame
    logger.info(f"Applying metric_id generation to DataFrame")
    df["metric_id"] = df.apply(generate_metric_id, axis=1)

    # place metric_id in the first column
    cols = df.columns.tolist()
    cols.insert(0, cols.pop(cols.index("metric_id")))
    df = df[cols]

    # Save
    logger.info(f"Saving the DataFrame to {output_file_path}")
    df.to_csv(output_file_path, index=False)


if __name__ == "__main__":
    main()
