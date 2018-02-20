import bagit
import logging
import os


class SANCBagger(object):
    def __init__(self) -> None:
        self.bag_to_open = None    # type: str
        self.tree_to_bag = None    # type: str
        self.working_bag = None    # type: bagit.Bag
        self.validation_error_details = None    # type: dict
        self.validation_error_message = None    # type: str
        self.bagging_error = None

    def open_bag(self, bag_to_open: str) -> bool:
        self.bag_to_open = bag_to_open
        self.working_bag = bagit.Bag(self.bag_to_open)
        return True

    def validate_bag(self) -> bool:
        try:
            self.working_bag.validate(processes=8)
            return True
        except bagit.BagValidationError as e:
            self.validation_error_details = e.details
            self.validation_error_message = e.message
            return False

    def create_bag(self, tree_to_bag: str, metadata: dict) -> bool:
        self.tree_to_bag = tree_to_bag
        try:
            self.working_bag = bagit.make_bag(self.tree_to_bag, metadata, processes=8, checksum=["sha256"])
            self.working_bag.save()
        except bagit.BagError as e:
            self.bagging_error = e
            return False
        return True

    def which_error(self):
        if self.validation_error_message:
            return self.validation_error_message, self.validation_error_details
        if self.bagging_error:
            return "BagError", self.bagging_error

    def is_already_bagged(self, tree):
        if os.path.isfile(os.path.join(tree, "bagit.txt")):
            return True
        return False