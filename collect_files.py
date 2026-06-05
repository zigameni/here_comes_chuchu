from pathlib import Path
import fnmatch


def should_exclude(path: Path, exclude_patterns: list[str]) -> bool:
    path_str = str(path).replace("\\", "/")

    for pattern in exclude_patterns:
        if fnmatch.fnmatch(path.name, pattern):
            return True

        if fnmatch.fnmatch(path_str, pattern):
            return True

    return False


def generate_markdown(
    root_dir: str,
    output_file: str = "project_dump.md",
    exclude_patterns: list[str] | None = None,
):
    root = Path(root_dir).resolve()
    exclude_patterns = exclude_patterns or []

    files = []

    for path in root.rglob("*"):
        if path.is_file() and not should_exclude(path.relative_to(root), exclude_patterns):
            files.append(path)

    files.sort()

    with open(output_file, "w", encoding="utf-8") as md:
        md.write(f"# Project Snapshot\n\n")
        md.write(f"Root: `{root}`\n\n")

        md.write("## Included Files\n\n")
        for file in files:
            rel = file.relative_to(root)
            md.write(f"- `{rel}`\n")
        md.write("\n")

        md.write("---\n\n")

        for file in files:
            rel = file.relative_to(root)

            md.write(f"## {rel}\n\n")

            suffix = file.suffix.lstrip(".")
            language = suffix if suffix else "text"

            try:
                content = file.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                md.write("*Binary file skipped*\n\n")
                continue

            md.write(f"```{language}\n")
            md.write(content)
            if not content.endswith("\n"):
                md.write("\n")
            md.write("```\n\n")


if __name__ == "__main__":
    generate_markdown(
        root_dir=".",
        output_file="project_dump.md",
        exclude_patterns=[
            ".git/*",
            "venv/*",
            "user_data/*",
            "user_data_new/*",
            "__pycache__/*",
            "btc_bot_engineering_plan.md",
            "btcbot.*",
            "*.log",
            "*.jsonl",
            "*.md",
            "captures/*",
            "*.pyc",
            "*.zip",
            "logs/*",
            "*.jsonl",
            ".phase35_tos.pids",
            "Report.md",
            "btc_bot_engineering_plan.md",
            "collect_files.py",
            "exits_signals.jsonl",
            "exits_tos.jsonl",
            "fills_signals.jsonl",
            "fills_tos.jsonl",
            "metrics.jsonl"
        ],
    )