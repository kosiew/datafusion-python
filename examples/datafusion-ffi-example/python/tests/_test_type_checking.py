import pyarrow as pa
from datafusion import udaf, SessionContext
from datafusion.user_defined import Accumulator  # base class for aggregators

# Define a simple test accumulator for demonstration:
class TestAccumulator(Accumulator):
    def __init__(self) -> None:
        self.total = 0

    def state(self) -> list[pa.Scalar]:
        return [pa.scalar(self.total)]

    def update(self, *values: pa.Array) -> None:
        # Sum up integer values from the first argument
        self.total += sum(value.as_py() for value in values[0])

    def merge(self, states: list[pa.Array]) -> None:
        # Assumes the state is a list with one scalar integer per actor
        self.total += sum(state[0].as_py() for state in states)

    def evaluate(self) -> pa.Scalar:
        return pa.scalar(self.total)

# Create the test UDAF using TestAccumulator.
# Note: the overload taking (accum, input_types, return_type, state_type, volatility, name)
test_udaf = udaf(
    TestAccumulator,           # accumulator function or type producing an Accumulator object
    [pa.int64()],              # input types (list of one int64)
    pa.int64(),                # return type
    [pa.int64()],              # state type (list of one int64)
    "immutable",               # volatility indicator
    name="test_udaf"
)

# Register UDAF into a session context (if needed)
ctx = SessionContext()
ctx.register_udaf(test_udaf)

# The code should type check without error:
print("Type checking passed for test_udaf!")