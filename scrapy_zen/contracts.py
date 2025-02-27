from typing import Any
from scrapy.contracts import Contract
from scrapy.exceptions import ContractFail
from itemadapter import ItemAdapter, is_item


class ScrapesContract(Contract):
    """Contract to check presence of fields in scraped items
    @scrapes page_name page_body
    """

    name = "scrapes"

    def post_process(self, output: list[Any]) -> None:
        for x in output:
            if is_item(x):
                item = ItemAdapter(x)
                missing = [arg for arg in self.args if arg not in item]
                if missing:
                    missing_fields = ", ".join(missing)
                    raise ContractFail(f"Missing fields: {missing_fields}")
                for arg in self.args:
                    if not item.get(arg):
                        raise ContractFail(f"Missing value for field: {arg}")