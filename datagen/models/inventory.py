class Inventory:
    def __init__(
        self,
        event_time: str,
        product_id: str,
        state: str,
        existing_level: int,
        stock_quantity: int,
        new_level: int,
    ):
        self.event_time = str(event_time)
        self.product_id = str(product_id)
        self.state = str(state)
        self.existing_level = int(existing_level)
        self.stock_quantity = int(stock_quantity)
        self.new_level = int(new_level)

    def __str__(self):
        return (
            "Inventory: event_time: {0}, product_id: {1}, state: {2}, existing_level: {3:.0f}, stock_quantity: {4:.0f}, "
            "new_level: {5:.0f}".format(
                self.event_time,
                self.product_id,
                self.state,
                self.existing_level,
                self.stock_quantity,
                self.new_level,
            )
        )
