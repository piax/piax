/*
 * DependencyVisitor.java - ASM visitors
 * 
 * Copyright (c) 2009-2015 PIAX development team
 * Copyright (c) 2006-2008 Osaka University
 * Copyright (c) 2004-2005 BBR Inc, Osaka University
 * 
 * Permission is hereby granted, free of charge, to any person obtaining 
 * a copy of this software and associated documentation files (the 
 * "Software"), to deal in the Software without restriction, including 
 * without limitation the rights to use, copy, modify, merge, publish, 
 * distribute, sublicense, and/or sell copies of the Software, and to 
 * permit persons to whom the Software is furnished to do so, subject to 
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY 
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
/*
 * Revision History:
 * ---
 * 2007/01/12 implemented by M. Yoshida.
 *       this code is modified from the ASM3.0 samples.
 * 2015/07/29 rewritten for asm-5.0 API by teranisi 
 * $Id: DependencyVisitor.java 718 2013-07-07 23:49:08Z yos $
 */
package org.piax.agent.impl.asm;

import java.util.HashSet;
import java.util.Set;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.Attribute;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.TypePath;
import org.objectweb.asm.signature.SignatureReader;
import org.objectweb.asm.signature.SignatureVisitor;

/**
 * Implementation class of ASM visitors.
 */
public class DependencyVisitor extends ClassVisitor {

    private Set<String> referedClasses;
    private String myClassName; // binary name

    public DependencyVisitor() {
    		super(Opcodes.ASM5);
        referedClasses = new HashSet<String>();
        myClassName = "";
    }

    public Set<String> getReferedClasses() {
        return referedClasses;
    }

    public void clearClasses() {
        referedClasses.clear();
    }

    // ClassVisitor

    public void visit(final int version, final int access, final String name,
            final String signature, final String superName,
            final String[] interfaces) {
        myClassName = name.replace('/', '.');

        if (signature == null) {
            addName(superName);
            addNames(interfaces);
        } else {
            addSignature(signature);
        }
    }

    class AnnotationDependencyVisitor extends AnnotationVisitor {

        public AnnotationDependencyVisitor() {
            super(Opcodes.ASM5);
        }

        @Override
        public void visit(final String name, final Object value) {
            if (value instanceof Type) {
                addType((Type) value);
            }
        }

        @Override
        public void visitEnum(final String name, final String desc,
                final String value) {
            addDesc(desc);
        }

        @Override
        public AnnotationVisitor visitAnnotation(final String name,
                final String desc) {
            addDesc(desc);
            return this;
        }

        @Override
        public AnnotationVisitor visitArray(final String name) {
            return this;
        }
    }

    public AnnotationVisitor visitAnnotation(final String desc,
            final boolean visible) {
        addDesc(desc);
        return new AnnotationDependencyVisitor();
    }

    public void visitAttribute(final Attribute attr) {
    }

    class FieldDependencyVisitor extends FieldVisitor {

        public FieldDependencyVisitor() {
            super(Opcodes.ASM5);
        }

        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            addDesc(desc);
            return new AnnotationDependencyVisitor();
        }

        @Override
        public AnnotationVisitor visitTypeAnnotation(final int typeRef,
                final TypePath typePath, final String desc,
                final boolean visible) {
            addDesc(desc);
            return new AnnotationDependencyVisitor();
        }
    }

    
    public FieldVisitor visitField(final int access, final String name,
            final String desc, final String signature, final Object value) {
        if (signature == null) {
            addDesc(desc);
        } else {
            addTypeSignature(signature);
        }
        if (value instanceof Type) {
            addType((Type) value);
        }
        return new FieldDependencyVisitor();
    }
    
    class MethodDependencyVisitor extends MethodVisitor {

        public MethodDependencyVisitor() {
            super(Opcodes.ASM5);
        }

        @Override
        public AnnotationVisitor visitAnnotationDefault() {
            return new AnnotationDependencyVisitor();
        }

        @Override
        public AnnotationVisitor visitAnnotation(final String desc,
                final boolean visible) {
            addDesc(desc);
            return new AnnotationDependencyVisitor();
        }

        @Override
        public AnnotationVisitor visitTypeAnnotation(final int typeRef,
                final TypePath typePath, final String desc,
                final boolean visible) {
            addDesc(desc);
            return new AnnotationDependencyVisitor();
        }

        @Override
        public AnnotationVisitor visitParameterAnnotation(final int parameter,
                final String desc, final boolean visible) {
            addDesc(desc);
            return new AnnotationDependencyVisitor();
        }

        @Override
        public void visitTypeInsn(final int opcode, final String type) {
        		if (type.charAt(0) == '[') {
                addDesc(type);
            } else {
                addName(type);
            }
            addType(Type.getObjectType(type));
        }

        @Override
        public void visitFieldInsn(final int opcode, final String owner,
                final String name, final String desc) {
        		addName(owner);
            addDesc(desc);
        }

        @Override
        public void visitMethodInsn(final int opcode, final String owner,
                final String name, final String desc, final boolean itf) {
        		if (owner.charAt(0) == '[') {
        			addDesc(owner);
        		} else {
        			addName(owner);
        		}
        		addMethodDesc(desc);
        }

        @Override
        public void visitInvokeDynamicInsn(String name, String desc,
                Handle bsm, Object... bsmArgs) {
        		// XXX this method was not found at asm 3.2
            addMethodDesc(desc);
        }

        @Override
        public void visitLdcInsn(final Object cst) {
        		if (cst instanceof Type) {
        			addType((Type) cst);
        		}
        }

        @Override
        public void visitMultiANewArrayInsn(final String desc, final int dims) {
            addDesc(desc);
        }

        @Override
        public AnnotationVisitor visitInsnAnnotation(int typeRef,
                TypePath typePath, String desc, boolean visible) {
            addDesc(desc);
            return new AnnotationDependencyVisitor();
        }

        @Override
        public void visitLocalVariable(final String name, final String desc,
                final String signature, final Label start, final Label end,
                final int index) {
            addTypeSignature(signature);
        }

        @Override
        public AnnotationVisitor visitLocalVariableAnnotation(int typeRef,
                TypePath typePath, Label[] start, Label[] end, int[] index,
                String desc, boolean visible) {
            addDesc(desc);
            return new AnnotationDependencyVisitor();
        }

        @Override
        public void visitTryCatchBlock(final Label start, final Label end,
                final Label handler, final String type) {
        		addName(type);
        }

        @Override
        public AnnotationVisitor visitTryCatchAnnotation(int typeRef,
                TypePath typePath, String desc, boolean visible) {
            addDesc(desc);
            return new AnnotationDependencyVisitor();
        }
    }

    public MethodVisitor visitMethod(final int access, final String name,
            final String desc, final String signature, final String[] exceptions) {
        if (signature == null) {
            addMethodDesc(desc);
        } else {
            addSignature(signature);
        }
        addNames(exceptions);
        return new MethodDependencyVisitor();
    }

    public void visitSource(final String source, final String debug) {
    }

    public void visitInnerClass(final String name, final String outerName,
            final String innerName, final int access) {
        /*
         * outerName か innerName に null が入ってくる場合を考慮
         * 無名内部クラスを使ったケースへの対応
         * 
         * suggestion from Dr. K. Abe (2008/10/24)
         */
        if (outerName != null && innerName != null) {
            addName(outerName + "$" + innerName);
        }
    }

    public void visitOuterClass(final String owner, final String name,
            final String desc) {
        //addName(owner);
        //addMethodDesc(desc);
    }

    class SignatureDependencyVisitor extends SignatureVisitor {

        public SignatureDependencyVisitor() {
            super(Opcodes.ASM5);
        }

        @Override
        public void visitClassType(final String name) {
        		addName(name);
        }

        @Override
        public void visitInnerClassType(final String name) {
            addName(name);
        }
    }

    // ---------------------------------------------

    private void addName(final String name) {
        if (name == null) {
            return;
        }
        String bname = name.replace('/', '.');
        if (bname.equals(myClassName)) {
            return;
        }
        referedClasses.add(bname);
    }

    private void addNames(final String[] names) {
        if (names == null) {
            return;
        }
        for (String name : names) {
            addName(name);
        }
    }

    private void addDesc(final String desc) {
        addType(Type.getType(desc));
    }

    private void addMethodDesc(final String desc) {
        addType(Type.getReturnType(desc));
        Type[] types = Type.getArgumentTypes(desc);
        for (int i = 0; i < types.length; i++) {
            addType(types[i]);
        }
    }

    private void addType(final Type t) {
        switch (t.getSort()) {
        case Type.ARRAY:
            addType(t.getElementType());
            break;
        case Type.OBJECT:
            addName(t.getClassName().replace('.', '/'));
            break;
        }
    }

    private void addSignature(final String signature) {
        if (signature != null) {
            new SignatureReader(signature).accept(new SignatureDependencyVisitor());
        }
    }

    private void addTypeSignature(final String signature) {
        if (signature != null) {
            new SignatureReader(signature).acceptType(new SignatureDependencyVisitor());
        }
    }
}
