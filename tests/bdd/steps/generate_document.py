import base64
from behave import given, when, then
import json
import os.path
from corvus_python.word_document_generator.word_document_generator import WordDocumentGenerator

generator = WordDocumentGenerator()
dirname = os.path.dirname(__file__)


@given(u"I have a template file '{template_file}'")
def i_have_a_template_file(context, template_file):
    template = generator.get_template(os.path.join(dirname, "../word_document_generator/input", template_file))
    context.template = template


@given(u"I read the template context from '{context_file}'")
def i_read_the_template_context_from(context, context_file):
    template_context = json.load(open(os.path.join(dirname, "../word_document_generator/input", context_file), 'r'))
    context.template_context = template_context


@given(u"I insert an inline image for '{image_file}' into the context as '{context_key}'")
def i_insert_an_inline_image_for_into_the_context_as(context, image_file, context_key):
    _insert_an_inline_image_with_height_and_width_for_into_the_context_as(context, image_file, context_key)


@given(u"I insert an inline image for '{image_file}' with height {height_mm:d}mm into the context as '{context_key}'")
def i_insert_an_inline_image_with_height_for_into_the_context_as(context, image_file, height_mm, context_key):
    _insert_an_inline_image_with_height_and_width_for_into_the_context_as(
        context, image_file, context_key, height_mm=height_mm)


@given(u"I insert an inline image for '{image_file}' with width {width_mm:d}mm into the context as '{context_key}'")
def i_insert_an_inline_image_with_width_for_into_the_context_as(context, image_file, width_mm, context_key):
    _insert_an_inline_image_with_height_and_width_for_into_the_context_as(
        context, image_file, context_key, width_mm=width_mm)


@given(u"I insert an inline image for '{image_file}' with height {height_mm:d}mm and width {width_mm:d}mm "
       "into the context as '{context_key}'")
def i_insert_an_inline_image_with_height_and_width_for_into_the_context_as(
        context,
        image_file,
        context_key,
        height_mm=None,
        width_mm=None):
    _insert_an_inline_image_with_height_and_width_for_into_the_context_as(
        context, image_file, context_key, height_mm=height_mm, width_mm=width_mm)


def _insert_an_inline_image_with_height_and_width_for_into_the_context_as(
        context,
        image_file,
        context_key,
        height_mm=None,
        width_mm=None):

    with open(os.path.join(dirname, "../word_document_generator/input", image_file), "rb") as i:
        image_base64 = base64.b64encode(i.read())
    context.template_context[context_key] = generator.get_inline_image_node(
        context.template,
        image_base64,
        height_mm,
        width_mm)


@when(u"I generate a Word document named '{output_file}'")
def i_generate_a_word_document_named(context, output_file):
    context.output_file = os.path.join(dirname, "../word_document_generator/output", output_file)

    # generate word document
    result = generator.render_doc_as_bytes(context.template, context.template_context)

    # save to file
    with open(context.output_file, 'wb') as f:
        f.write(result)


@then(u"the Word document should be generated")
def the_word_document_should_be_generated(context):
    # check that the file exists
    assert os.path.isfile(context.output_file)
