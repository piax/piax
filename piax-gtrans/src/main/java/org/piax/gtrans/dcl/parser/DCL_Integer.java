/* Generated By:JJTree: Do not edit this line. DCL_Integer.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=DCL_,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package org.piax.gtrans.dcl.parser;

public
@SuppressWarnings("all")
class DCL_Integer extends SimpleNode {
  public DCL_Integer(int id) {
    super(id);
  }

  public DCL_Integer(DCLParser p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public Object jjtAccept(DCLParserVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=b001333e4457823e1a9211323b99c319 (do not edit this line) */