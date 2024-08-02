class DataRetrieverException(Exception):
    pass


class NoMeteringOrMarketLocationFound(DataRetrieverException):
    def __init__(self, market_location_number: str):
        super().__init__(
            f"No MeteringOrMarketLocation found for {market_location_number}"
        )


class ConflictingEnergyData(DataRetrieverException):
    def __init__(self, market_location_number: str):
        super().__init__(
            f"Found multiple MeteringOrMarketLocations with different energy data for number {market_location_number}"
        )
