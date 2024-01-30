@word_document_generator
Feature: Generate Word document from template

    Scenario: Generate Word document from template
        Given I have a template file 'template1.docx'
        And I read the template context from 'context1.json'
        When I generate a Word document named 'output1.docx'
        Then the Word document should be generated
        # We should also check the content of the generated document but it's not easy to do