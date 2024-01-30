import glob
import os


def before_feature(context, feature):
    if "skip" in feature.tags:
        feature.skip("Marked with @skip")
        return

    if "word_document_generator" in feature.tags:
        # delete *.docx files in word_document_generator/output directory
        dirname = os.path.dirname(__file__)
        files = glob.glob(os.path.join(dirname, 'word_document_generator/output/*.docx'))
        for f in files:
            os.remove(f)
        return


def before_scenario(context, scenario):
    if "skip" in scenario.effective_tags:
        scenario.skip("Marked with @skip")
        return


def after_scenario(context, scenario):
    # Clean-up tables
    pass
