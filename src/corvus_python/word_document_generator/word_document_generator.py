import base64
from io import BytesIO
from os import PathLike
from typing import IO, Dict
from docxtpl import DocxTemplate, InlineImage
from docx.shared import Mm


class WordDocumentGenerator:
    """A class for generating Word documents from templates."""

    def render_doc_as_bytes(self, template: DocxTemplate, context: Dict[str, any], autoescape: bool = False) -> bytes:
        """Render a Word docx document from the provided template and context,
        and return the result as a byte array.

        Args:
            template (DocxTemplate): The template to render. Use `get_template` to get the template object.
            context (Dict[str, any]): The context to render the template with.
            autoescape (bool, optional): Whether to autoescape characters when rendering. Defaults to False.

        Returns:
            bytes: The rendered document as a byte array.
        """

        template.render(context, autoescape=autoescape)

        try:
            # save to byte array
            stream = BytesIO()
            template.save(stream)
            result = stream.getvalue()

        finally:
            # dispose of the stream
            stream.close()

        return result

    def get_template(self, template_file: IO[bytes] | str | PathLike) -> DocxTemplate:
        """Get a template object from the provided file.
        The file must be a `.docx` file. See https://docxtpl.readthedocs.io/en/latest/
        for more information on the templating language.

        Args:
            template_file (IO[bytes] | str | PathLike): The file to get the template from.
                Can be a file path, a file-like object, or a byte array.

        Returns:
            DocxTemplate: The template object.
        """
        return DocxTemplate(template_file)

    def get_inline_image_node(
            self,
            template: DocxTemplate,
            base64_image: str,
            height_mm: float = None,
            width_mm: float = None) -> InlineImage:
        """Get an inline image node to set on the template context, from the provided base64 encoded image string.
        If height and width are not provided, the image will be rendered at its original size.
        If only one of height or width is provided, the image will be scaled to the provided dimension.

        Args:
            template (DocxTemplate): The template to render the image in.
            base64_image (str): The base64 encoded image string.
            height_mm (float, optional): The height in millimetres to render the image. Defaults to None.
            width_mm (float, optional): The width in millimetres to render the image. Defaults to None.

        Returns:
            InlineImage: The inline image node.
        """

        image_bytes = BytesIO(base64.b64decode(base64_image))

        width = Mm(width_mm) if width_mm is not None else None
        height = Mm(height_mm) if height_mm is not None else None

        return InlineImage(
            template,
            image_bytes,
            width=width,
            height=height
        )
