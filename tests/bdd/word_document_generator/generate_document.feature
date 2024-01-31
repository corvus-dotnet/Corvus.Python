@word_document_generator
Feature: Generate Word document from template

    Scenario: Generate Word document from template
        Given I have a template file 'template1.docx'
        And I read the template context from 'context1.json'
        When I generate a Word document named 'output1.docx'
        Then the Word document should be generated
        # We should also check the content of the generated document but it's not easy to do

    Scenario: Generate Word document from template with an image
        Given I have a template file 'template2.docx'
        And I read the template context from 'context1.json'
        And I insert an inline image for 'image1.jpg' into the context as 'profile_image'
        When I generate a Word document named 'output2.docx'
        Then the Word document should be generated
        # We should also check the content of the generated document but it's not easy to do

    Scenario: Generate Word document from template with an image with explicit height and width
        Given I have a template file 'template2.docx'
        And I read the template context from 'context1.json'
        And I insert an inline image for 'image1.jpg' with height 50mm and width 40mm into the context as 'profile_image'
        When I generate a Word document named 'output3.docx'
        Then the Word document should be generated
        # We should also check the content of the generated document but it's not easy to do

    Scenario: Generate Word document from template with an image with explicit height
        Given I have a template file 'template2.docx'
        And I read the template context from 'context1.json'
        And I insert an inline image for 'image1.jpg' with height 50mm into the context as 'profile_image'
        When I generate a Word document named 'output4.docx'
        Then the Word document should be generated
        # We should also check the content of the generated document but it's not easy to do

    Scenario: Generate Word document from template with an image with explicit width
        Given I have a template file 'template2.docx'
        And I read the template context from 'context1.json'
        And I insert an inline image for 'image1.jpg' with width 50mm into the context as 'profile_image'
        When I generate a Word document named 'output5.docx'
        Then the Word document should be generated
        # We should also check the content of the generated document but it's not easy to do