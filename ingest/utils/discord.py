import os
import requests
from jinja2 import Environment, FileSystemLoader


class DiscordLogger:
    def __init__(self, webhook_url: str) -> None:
        self.WEBHOOK_URL = webhook_url
        self.env = Environment(loader=FileSystemLoader('templates'))


    def render_table_report(self, table_obj: dict, extract_date: str):
        template = self.env.get_template('table-report.md')
        return template.render(extract_date=extract_date, table=table_obj)


    def log_table_report(self, table_obj: dict, extract_date: str):
        output_from_parsed_template = self.render_table_report(table_obj, extract_date)
        return requests.post(self.WEBHOOK_URL, {"content": output_from_parsed_template})
    

    def render_source_report(self, table_report_list: str, source_name: str, extract_date: str):
        template = self.env.get_template('source-report.md')
        return template.render(table_report_list=table_report_list, source_name=source_name, extract_date=extract_date)


    def log_source_report(self, table_report_list: str, source_name: str, extract_date: str):
        output_from_parsed_template = self.render_source_report(table_report_list=table_report_list, source_name=source_name, extract_date=extract_date)
        # splitting into reports with less than 2000 characters (discord character restraint)
        report_list = output_from_parsed_template.split('##')
        report_title = report_list.pop(0)
        final_report_list = [report_title]
        i=0
        for report in report_list:
            if len(f"{final_report_list[i]}##{report}") < 2000:
                final_report_list[i] = f"{final_report_list[i]}##{report}"
            else:
                i=i+1
                final_report_list.append(f'##{report}')

        for index, report in enumerate(final_report_list):
            try:
                with open(f'compiled/{extract_date}-{index}-{source_name}.md', 'w') as f:
                    f.write(report)
            except FileNotFoundError:
                os.mkdir('compiled')
                with open(f'compiled/{extract_date}-{index}-{source_name}.md', 'w') as f:
                    f.write(report)
            finally:
                requests.post(self.WEBHOOK_URL, {"content": report})
