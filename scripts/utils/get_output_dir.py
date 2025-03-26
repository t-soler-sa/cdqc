# get_output_dir.py
from pathlib import Path


def get_output_dir(
    script_name: str, sri_data_dir: Path, interactive: bool = True
) -> Path:
    """
    Determines and creates the output directory based on the script name.

    Parameters:
        script_name (str): The name of the script.
        sri_data_dir (Path): The base directory (as a pathlib.Path) where the output directory will be created.
        interactive (bool): If False, user prompts will be disabled and a default directory name will be used.
                            Defaults to True.

    Returns:
        Path: The full path of the output directory.
    """
    mapping = {
        "zombie-killer": "zombie_list",
        "pre_ovr_analysis": "pre_ovr_analysis",
        "00_preovr_analysis_str_level": "pre_ovr_analysis",
        "overrides": "overrides",
        "noncompliance": "noncompliance",
        "impact_analysis": "impact_analysis",
    }

    script_lower = script_name.lower().strip()
    if script_lower in mapping:
        dir_name = mapping[script_lower]
    elif not interactive:
        # In non-interactive mode, default to script_lower
        dir_name = script_lower
    else:
        # Ask user if it's okay to create a new directory
        user_response = (
            input(
                f"No dir was found. A new dir will be created with the name '{script_lower}'. Are you OK with that? (y/n): "
            )
            .strip()
            .lower()
        )

        if user_response in ("y", "yes"):
            dir_name = script_lower
        else:
            options = [
                "pre_ovr_analysis",
                "zombie_list",
                "overrides",
                "noncompliance",
                "impact_analysis",
            ]
            print("Please choose one of the following options:")
            for idx, option in enumerate(options, 1):
                print(f"{idx} - {option}")

            while True:
                selection = input("Enter a number (1-5): ").strip()
                if selection in [str(i) for i in range(1, len(options) + 1)]:
                    dir_name = options[int(selection) - 1]
                    break
                else:
                    print("Invalid selection. Please enter a number between 1 and 5.")

    output_dir = sri_data_dir / dir_name

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"Error creating directory {output_dir}: {e}")
        raise

    print(f"Output directory for script {script_name} is set to: {output_dir}")
    return output_dir
