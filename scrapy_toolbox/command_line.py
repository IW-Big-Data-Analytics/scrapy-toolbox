import subprocess
import sys
import click
import string
import os
from pathlib import Path


templates_all = ["models.py.tmpl",
                 "pipelines.py.tmpl",
                 "settings.py.tmpl"]

location_of_file: Path = Path(__file__)


def _render_template(file, **kwargs):
    with open(file, "r") as f:
        raw = f.read()
    content = string.Template(raw).substitute(**kwargs)
    path = os.path.basename(file).replace(".tmpl", "")
    return content, path

@click.group()
def cli():
    pass


@cli.command()
@click.argument("spider")
def check_output(spider):
    """
    Check output for spider.
    :param spider: name of spider.
    """
    if spider:
        subprocess.check_output(["scrapy", "crawl", spider, "-a process_errors=True"])

@cli.command()
@click.argument("projectname")
def startproject(projectname: str):
    """
    Extends the scrapy startproject command by adding specific files and lines for scrapy-toolbox.
    :param projectname: Name of the project.
    """
    path_templates = location_of_file.parent.parent.joinpath("templates")
    if not path_templates.exists():
        raise FileNotFoundError("Template File is missing.")
    Projectname = projectname.capitalize().replace("_", "").replace("-", "")
    subprocess.check_output(f"scrapy startproject {projectname} > /dev/null")
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
    New Scrapy project 'hi' created in: {os.path.abspath(os.curdir) + "/" + projectname}
    You can start your first spider with:
    cd {projectname}
    scrapy-toolbox genspider example example.com
    """)


@cli.command()
@click.argument("spidername")
@click.argument("domain")
def genspider(spidername: str, domain: str):
    """
    Extends scrapy genspider command and add imports and metaclass to spider.
    :param spidername: Name of the spider.
    :param domain: Domain for spider.
    """
    assert "scrapy.cfg" in os.listdir(), "not a scrapy directory. Use scrapy-toolbox startproject to create one."
    os.system(f"scrapy genspider {spidername} {domain}")
    path_file = os.path.abspath(__file__).replace("/commands/commands.py", "")
    classname = spidername.capitalize().replace("-", "").replace("_", "")
    project_name = os.path.basename(os.path.abspath(os.curdir))
    content, path = _render_template(
        file=f"{path_file}/templates/toolbox_spider.py.tmpl",
        name=spidername,
        domain=domain,
        classname=classname
    )
    path = path.replace("toolbox_spider", spidername)
    with open(f"{project_name}/spiders/{path}", "w") as f:
        f.write(content)
    click.echo(f"Created spider '{spidername}' using template 'toolbox_spider' ")

