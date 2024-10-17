from typing import Set, Tuple, List


class UniqueTupleExtractor:
    def __init__(self, j_set: Set[str], i_set_ordered: Set[int], t_set_ordered: Set[int]):
        self.j_set = j_set
        self.i_set_ordered = sorted(i_set_ordered)  # if not yet ordered
        self.t_set_ordered = sorted(t_set_ordered)  # if not yet ordered

    def get_unique_combinations(self, set_names: List[str]) -> Set[Tuple]:
        if len(set_names) == 2:
            if "j" in set_names and "i" in set_names:
                return set((j, i) for j in self.j_set for i in self.i_set_ordered)
            elif "j" in set_names and "t" in set_names:
                return set((j, t) for j in self.j_set for t in self.t_set_ordered)
            elif "i" in set_names and "t" in set_names:
                return set((i, t) for i in self.i_set_ordered for t in self.t_set_ordered)
        elif len(set_names) == 3:
            return set((j, i, t) for j in self.j_set for i in self.i_set_ordered for t in self.t_set_ordered)
        else:
            raise ValueError("You need to specify either 2 or 3 sets for combinations.")

    def get_unique_tuples(self) -> Set[Tuple[str, int, int]]:
        return set((j, i, t) for j in self.j_set for i in self.i_set_ordered for t in self.t_set_ordered)

