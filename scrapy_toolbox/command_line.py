import subprocess
import click
import string
import os
from pathlib import Path
from typing import Tuple


templates_all = ["models.py.tmpl",
                 "pipelines.py.tmpl",
                 "settings.py.tmpl"]

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

location_of_file: Path = Path(__file__)
path_templates = location_of_file.parent.parent.joinpath("templates")


# Use python setup.py develop to test.

def _render_template(file, **kwargs) -> Tuple[str, str]:
    with open(file, "r") as f:
        raw = f.read()
    content = string.Template(raw).substitute(**kwargs)
    path = os.path.basename(file).replace(".tmpl", "")
    return content, path


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass


@cli.command()
@click.argument("spidername", required=True, nargs=1)
def check_output(spidername: str):
    """
    Check output for spider.
    spidername: name of spider
    """
    subprocess.check_output(["scrapy", "crawl", spidername, "-a process_errors=True"])

@cli.command()
@click.argument("projectname", required=True, nargs=1)
def startproject(projectname: str):
    """
    Generate project for scrapy-toolbox.
    projectname: Name of new project
    """
    if not path_templates.exists():
        raise FileNotFoundError("Template File is missing.")
    Projectname = projectname.capitalize().replace("_", "").replace("-", "")
    os.system(f"scrapy startproject {projectname} > /dev/null")
    for template_file in templates_all:
        template_absolute_path = path_templates.joinpath(template_file)
        if not template_absolute_path.exists():
            raise FileNotFoundError(f"Templatefile {template_file} not found")
        content, path = _render_template(
            file=template_absolute_path,
            ProjectName=Projectname,
            project_name=projectname)
        with open(f"{projectname}/{projectname}/{path}", "w") as f:
            f.write(content)
    click.echo(f"""
    New Scrapy project '{projectname}' created in: {os.curdir + "/" + projectname}
    You can start your first spider with:
    cd {projectname}
    scrapy-toolbox genspider example example.com
    """)


@cli.command()
@click.argument("spidername", nargs=1)
@click.argument("domain", default="Example.com", nargs=1)
def genspider(spidername: str, domain: str):
    """
    Create spider that supports error handling.
    spidername: name of new spider.
    domain: domain to scrape (default: Example.com).
    """
    assert "scrapy.cfg" in os.listdir(), "not a scrapy directory. Use scrapy-toolbox startproject to create one " \
                                         "or navigate to project directory."
    os.system(f"scrapy genspider {spidername} {domain}")
    if not path_templates.exists():
        raise FileNotFoundError("Template File is missing.")
    spider_template = path_templates.joinpath("toolbox_spider.py.tmpl")
    if not spider_template.exists():
        raise FileNotFoundError("Spider Template File is missing.")
    classname = spidername.capitalize().replace("-", "").replace("_", "")
    project_name = os.path.basename(os.path.abspath(os.curdir))
    content, path = _render_template(
        file=path_templates.joinpath("toolbox_spider.py.tmpl"),
        name=spidername,
        domain=domain,
        classname=classname
    )
    path = path.replace("toolbox_spider", spidername)
    with open(f"{project_name}/spiders/{path}", "w") as f:
        f.write(content)
    click.echo(f"Created spider '{spidername}' using template 'toolbox_spider' ")

