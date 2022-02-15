package com.ttd.contextual.datasets

import scala.collection.immutable.List
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/**
 * A macro to produce a TreeSet of all instances of a sealed trait.
 * Based on Travis Brown's work:
 * http://stackoverflow.com/questions/13671734/iteration-over-a-sealed-trait-in-scala
 * CAREFUL: !!! MUST be used at END OF code block containing the instances !!!
 */
object EnumerationMacros {
  def sealedInstancesOf[A]: List[A] = macro sealedInstancesOf_impl[A]

  def sealedInstancesOf_impl[A: c.WeakTypeTag](c: blackbox.Context): c.Expr[List[A]] = {
    import c.universe._

    val symbol: c.universe.ClassSymbol = weakTypeOf[A].typeSymbol.asClass

    if  (!symbol.isClass || !symbol.isSealed)
      c.abort(c.enclosingPosition, "Can only enumerate values of a sealed trait or class.")
    else {

      val children = symbol.knownDirectSubclasses.toList

      if (!children.forall(_.isModuleClass)) c.abort(c.enclosingPosition, "All children must be objects.")
      else c.Expr[List[A]] {

        def sourceModuleRef(sym: Symbol) = Ident(sym.asInstanceOf[scala.reflect.internal.Symbols#Symbol
        ].sourceModule.asInstanceOf[Symbol]
        )

        Apply(
          Select(
            reify(List).tree,
            TermName("apply")
          ),
          children.map(sourceModuleRef(_))
        )
      }
    }
  }
}