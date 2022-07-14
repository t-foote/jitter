from typing import *
from math import floor
from csv import writer


class LogTree:
    pass


class LogTree:
    _message_id:    Optional[int]
    _period:        Optional[Dict[int, int]]
    _timestamps:    Optional[Dict[int, List[float]]]
    _left:          Optional[LogTree]
    _right:         Optional[LogTree]

    def __init__(self, period_dict: Dict[int, int] = None, timestamps_dict: Dict[int, List[float]] = None):
        """
        The initializer function takes 2 arguments: period_dict & timestamps_dict:
        - period_dict's keys should be Message IDs and its values should be the Message ID's period.
        - timestamps_dict's keys should be Message IDs and its values should be a list of that Message ID's timestamps.

        Below this class, there is a helper function import_files(), which takes 2 filenames (in this case, logdata.csv
        and periods.csv) and returns a LogTree object.

        Pre-conditions:
        - keys(period_dict) == keys(timestamps_dict)
          (In other words, the message IDs are the same in both dictionaries.)
        - If period_dict is None, then timestamps_dict is None.
          (This makes an empty LogTree.)
        """
        if period_dict is None and timestamps_dict is None:
            """This makes an empty tree."""
            self._timestamps = None
            self._period = None
            self._message_id = None
            self._left = None
            self._right = None
            return None

        if period_dict is None or timestamps_dict is None:
            raise Exception("Arguments must either both be file names or both be NoneType.")

        self._message_id = sorted(period_dict.items())[floor(len(period_dict)/2)][0]
        self._period = period_dict[self._message_id]
        self._timestamps = timestamps_dict[self._message_id]

        if len(period_dict) == 1:
            self._left = LogTree()
            self._right = LogTree()
        elif len(period_dict) == 2:
            self._left = LogTree(
                dict(sorted(period_dict.items())[:floor(len(period_dict) / 2)]),
                dict(sorted(timestamps_dict.items())[:floor(len(period_dict) / 2)])
            )
            self._right = LogTree()
        elif len(period_dict) > 2:
            self._left = LogTree(
                dict(sorted(period_dict.items())[:floor(len(period_dict)/2)]),
                dict(sorted(timestamps_dict.items())[:floor(len(period_dict)/2)])
            )
            self._right = LogTree(
                dict(sorted(period_dict.items())[floor(len(period_dict)/2)+1:]),
                dict(sorted(timestamps_dict.items())[floor(len(period_dict)/2)+1:])
            )

    def is_empty(self) -> bool:
        """Returns True iff self is an 'empty tree'."""
        return self._message_id is None

    def __str__(self, depth: int = 0) -> str:
        if self.is_empty():
            return ""
        string = ":   " * depth + str(self._message_id) + "\n"
        for item in self._left, self._right:
            string += item.__str__(depth + 1)
        return string

    def __contains__(self, item) -> bool:
        if self.is_empty():
            return False
        elif item == self._message_id:
            return True
        elif item < self._message_id:
            return item in self._left
        else:
            return item in self._right

    def find(self, message_id: int) -> Optional[LogTree]:
        """Finds and returns the LogTree object of the specified message_id; or None if message_id isn't in the tree."""
        if not self.is_empty() and message_id in self:
            if message_id == self._message_id:
                return self
            elif message_id < self._message_id:
                return self._left.find(message_id)
            else:
                return self._right.find(message_id)

    def all_message_ids(self) -> Set[int]:
        """Returns a list of all Message IDs in the tree."""
        out = set()
        if self.is_empty():
            return out
        else:
            out.add(self._message_id)
            out = out.union(self._left.all_message_ids()).union(self._right.all_message_ids())
            return out

    def period(self, message_id: int = None) -> Optional[int]:
        """Returns the period of inputted message_id; or None if message_id isn't in the tree."""
        if message_id is None or message_id == self._message_id:
            return self._period
        if self.find(message_id) is not None:
            return self.find(message_id).period()

    def timestamps(self, message_id: int = None) -> Optional[List[float]]:
        """Returns a list of timestamps of inputted message_id; or None if message_id isn't in the tree."""
        if message_id is None or message_id == self._message_id:
            return self._timestamps
        if self.find(message_id) is not None:
            return self.find(message_id).timestamps()

    def accuracy(self, message_id: int = None) -> Optional[float]:
        """Returns an 'accuracy score' of a given message_id, which is the average absolute value difference between
        the expected period and the actual gap between messages; or None if message_id isn't in the tree."""
        if message_id is None:
            message_id = self._message_id
        out = 0
        t = self.timestamps(message_id)
        p = self.period(message_id)
        if message_id in self:
            for i in range(len(t) - 1):
                out += abs((t[i + 1] - t[i]) - p)
        return out / (len(t) - 1)

    def gaps(self, message_id: int = None) -> Optional[List[float]]:
        """Returns a list of all gaps (in milliseconds) between messages sent; or None if message_id isn't in the tree."""
        if message_id is None:
            message_id = self._message_id
        out = []
        t = self.timestamps(message_id)
        p = self.period(message_id)
        if message_id in self:
            for i in range(len(t) - 1):
                out.append(t[i + 1] - t[i] - p)
        return out

    def accuracy_report(self) -> Dict[int, float]:
        """Returns a dictionary of each message_id mapped to its accuracy score."""
        out = dict()
        for item in self.all_message_ids():
            out[item] = self.accuracy(item)
        return out

    def sorted_by_accuracy(self) -> List[int]:
        """Returns a list of all message_ids in the tree, sorted by accuracy from most to least accurate."""
        d = dict()
        out = []
        for item in self.all_message_ids():
            d[self.accuracy(item)] = item
        for item in sorted(d.items()):
            out.append(item[1])
        return out

    def frequency(self, message_id: int) -> int:
        """Returns how frequency a given message_id sent messages."""
        return len(self.timestamps(message_id))

    def all_frequencies(self) -> Dict[int, int]:
        """Returns a dictionary of all message_ids and their frequencies."""
        out = dict()
        for item in self.all_message_ids():
            out[item] = self.frequency(item)
        return out


def import_files(periods_filename: str, logdata_filename: str) -> LogTree:
    """This is a function that takes in 2 file names and returns a LogTree object. The periods_filename file 
    be formatted similar to periods.csv; and like-wise, the logdata_filename must be formatted similar to 
    logdata.csv. Both CSV files's first rows will be treated as header rows and thus ignored."""

    period_dict = dict()
    timestamps_dict = dict()

    for item in list(open(periods_filename))[1:]:
        period_dict[int(item.split(",")[0])] = int(item.split(",")[1])
        timestamps_dict[int(item.split(",")[0])] = []

    for item in list(open(logdata_filename))[1:]:
        if int(item.split(",")[1]) in period_dict:
            timestamps_dict[int(item.split(",")[1])].append(float(item.split(",")[0]))

    return LogTree(period_dict, timestamps_dict)


def dict_to_csv(csv_filename_to_write: str, dictionary: dict) -> None:
    """Takes the filename of a CSV file & a dictionary, and writes data from the dictionary into the CSV file."""
    w = writer(open(csv_filename_to_write, "w"))
    for i, j in dictionary.items():
        w.writerow([i, j])
    open(csv_filename_to_write, "w").close()


if __name__ == '__main__':
    log = import_files("periods.csv", "logdata.csv")
    print(log)
