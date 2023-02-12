from dataclasses import dataclass


@dataclass
class FrequentItemSet:
    attributes: dict
    risk_ratio: float
    occurrence_ratio: float
    support_ratio: float
    confidence: float

    def __instancecheck__(self, risk, occurrences, supp, conf, attributes):
        self.risk_ratio = risk
        self.support_ratio = supp
        self.occurrence_ratio = occurrences
        self.confidence = conf
        self.attributes = attributes

    def __lt__(self, other):
        return self.risk_ratio < other.risk_ratio
