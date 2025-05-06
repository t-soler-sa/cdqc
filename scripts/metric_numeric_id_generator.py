# metric_numeric_id_generator.py

"""
Script to assign metric_id to ESG metrics using hierarchical numeric pattern/structure
"""

import pandas as pd
from scripts.utils.config import get_config

config = get_config("metric_id_generator", auto_date=False, fixed_date="202505")
logger = config["logger"]

base_dir_path = config["ESG_METRICS_MAP_DIR"]
input_file_path = base_dir_path / "esg_metrics_temp.csv"
output_file_path = base_dir_path / "esg_dim_metrics.csv"

# Mapping dictionaries
provider_codes = {
    "Clarity.ai": "20",
    "NASDAQ": "30",
    "Sustainalytics": "40",
    "Santander Group": "10",
}

category_codes = {
    "Controversy Metrics": "12",
    "Expousure Metrics": "13",
    "Flags": "14",
    "Group Policies": "15",
}

data_type_codes = {
    "boolean": "01",
    "string": "02",
    "int": "03",
    "float": "04",
    "long": "05",
    "double": "06",
    "decimal": "07",
    "date": "08",
    "timestamp": "09",
}

category_detail_codes = {
    "critical": "50",
    "high": "40",
    "medium": "30",
    "low": "20",
    "default_controversy": "10",
    "default": "00",
    "production": "01",
    "participation": "02",
    "flags": "07",
    "green_bond_flags": "77",
}

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

# Full topic_codes dictionary included (as provided by user)
# define major topics that agroup the themes

topic_codes = {
    # Environment-related issues including fossil fuels, emissions, resource use, and climate policy.
    "environment": {
        "topic_id": "110",  # Major ESG category: Environment
        "themes_list": [
            "040",
            "041",
            "042",
            "043",
            "044",
            "045",
            "046",
            "047",
            "048",  # Fossil fuels and extraction
            "049",
            "050",
            "051",
            "054",  # Agrochemicals and GMOs
            "067",
            "068",
            "069",
            "070",
            "071",
            "072",  # Emissions, effluents, water/land/biodiversity impacts
            "097",  # Climate policy
        ],
    },
    # Broad "S" themes including labor rights, community, and some animal/meat welfare themes.
    "social": {
        "topic_id": "120",  # Major ESG category: Social
        "themes_list": [
            "010",
            "011",  # Animal welfare and testing
            "052",
            "053",
            "061",  # Meat and exotic products
            "073",
            "074",
            "075",
            "076",  # Community and labor/human rights
            "083",
            "084",
            "085",  # Workplace safety and data privacy
        ],
    },
    # Weapons, arms, and military technologies—grouped as a controversial subcategory of "Social"
    "controversial_weapons": {
        "topic_id": "121",  # Subcategory under Social (120)
        "themes_list": [
            "012",  # Generic armament
            "020",
            "030",
            "031",
            "032",  # Weapons types (nuclear, chem/bio)
            "033",
            "034",
            "035",
            "036",
            "037",  # Specific banned/controversial weapons
        ],
    },
    # Faith-based or moral exclusions typically guided by religious or ethical norms
    "ethical_exclusions": {
        "topic_id": "122",  # Subcategory under Social (120)
        "themes_list": [
            "055",
            "056",
            "057",
            "058",  # Alcohol, tobacco, cannabis
            "059",
            "060",  # Gambling and pornography
            "062",
            "063",
            "064",
            "065",  # Reproductive and stem cell research
        ],
    },
    # Financial flags or issues like unethical lending and bonds
    "finance_specific": {
        "topic_id": "123",  # Subcategory under Social (120)
        "themes_list": [
            "066",  # Predatory lending
            "090",
            "091",  # Bond-related ESG flags
        ],
    },
    # Governance-related topics including corruption, taxation, board practices, market behavior, and ESG ratings
    "governance": {
        "topic_id": "130",  # Major ESG category: Governance
        "themes_list": [
            "077",
            "078",
            "079",
            "080",
            "081",
            "082",  # Governance, ethics, and IP
            "086",
            "087",
            "088",
            "089",  # Market behavior and sanctions
            "092",
            "093",
            "094",
            "095",
            "096",  # ESG ratings and scores
            "099",
            "101",  # Group/global compliance
        ],
    },
    # ESG-related policies that may not be explicitly environmental or governance but are still relevant to ESG management
    "esg_policy": {
        "topic_id": "131",  # Subcategory under Governance (130)
        "themes_list": [
            "098",  # Defense policy (linked to controversy)
            "097",  # Climate policy (also appears in environment)
            "099",  # Group-level policy
        ],
    },
    # Catch-all or generic codes like default values and highest controversy indicators
    "general": {
        "topic_id": "999",  # General/undefined category
        "themes_list": [
            "100",  # Highest controversy level
            "999",  # Default or undefined
        ],
    },
}


# Implemented Functions (unchanged, using provided dictionaries)
def get_theme_code(metric_name):
    return themes.get(metric_name.lower(), "999")


def get_topic_code(theme_code):
    for topic_details in topic_codes.values():
        if theme_code in topic_details["themes_list"]:
            return topic_details["topic_id"].zfill(3)
    return "999"


def get_category_detail_code(metric_category, metric_name, provider):
    name = metric_name.lower()
    if metric_category == "13":
        if name.endswith("_prod"):
            return "01"
        elif name.endswith("_part"):
            return "02"
        else:
            return "00"
    if metric_category == "12":
        for level in ["critical", "high", "medium", "low"]:
            if level in name:
                return category_detail_codes[level]
        return "10"
    if provider == "20" and "bond" in name:
        return "77"
    if (
        provider == "Sustainalytics"
        and "overall_global_compact_compliance_status" in name
    ):
        return "07"
    return "99"


def generate_metric_id(row):
    return f"'{provider_codes.get(row['data_provider'],'99')}{category_codes.get(row['metric_category'],'99')}{get_topic_code(get_theme_code(row['metric_name']))}{get_theme_code(row['metric_name'])}{get_category_detail_code(row['metric_category'],row['metric_name'],row['data_provider'])}{data_type_codes.get(row['metric_type'],'99')}"


def main():
    df = pd.read_csv(input_file_path)
    logger.info("Applying metric_id generation to DataFrame")
    df["metric_id"] = df.apply(generate_metric_id, axis=1)
    df = df[["metric_id"] + [col for col in df.columns if col != "metric_id"]]
    df.to_csv(output_file_path, index=False)
    logger.info(f"Metric IDs generated and saved to {output_file_path}")


if __name__ == "__main__":
    main()
