
** Important **

Don't forget to sign the Declaration of Academic Integrity / Eidesstattliche Erklärung at the end of the thesis document. The LaTeX file can be found in "included/declaration.tex", where the name (Firstname Lastname) should be adjusted to your name!
It is necessary to sign the German and English version due to the German law. 

Please, also fill out and sign the document about the exploitation rights / Verwertungsrechte to be found in the folder "Exploitation_Rights" and send the original to Prof. Dr. Markus Endres. 
It is necessary to sign the German and English version due to the German law. 



** Thesis Document Structure ** 


"thesis.tex" is the main LaTeX document. 
Do not modify the style, the title page or any other format of the template. 

It includes some "input" files based on the chapter structure of the thesis. You can add as many additional chapters as you need. 

Also feel free to add any LaTeX package necessary for your thesis. 

User Biber for literature references instead of bibtex. 

To compile your thesis use pdflatex and biber: 

pdflatex thesis.tex
biber thesis.bcf
pdflatex thesis.tex


If you want to write your thesis in German, than delete all occurence of "english" in thesis.tex to use a German "Contents" / "Inhaltsverzeichnis" and document structure. Also adjust "finalthesis.sty" to the "German" content. 
