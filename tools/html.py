""" Functiona to assist with manipulating and creating HTML 
        - create HTML body for email, use with gmail API
"""

# Create cahflow email recon html
def htmlTxtandTable(heading, df, style, paragaph = ""):
    # convert table as html
    df_html = df.to_html(index=False,classes=style)
    #read table style
    with open(f'styles/{style}.css', 'r') as cssfile:
        style = cssfile.read()
    #create html
    html = f"""
        <html>
            <head>{heading}
                <style>{style}</style>
            </head>
            <p>{paragaph}</p>
            {df_html}
        </html>
    """
    return(html)

def paragraphBuilder(p):
    p
    return(p)
