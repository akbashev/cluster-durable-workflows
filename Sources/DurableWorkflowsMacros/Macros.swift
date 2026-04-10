import Foundation
import SwiftCompilerPlugin
import SwiftSyntax
import SwiftSyntaxBuilder
import SwiftSyntaxMacros

public struct WorkflowMacro: ExtensionMacro {
  public static func expansion(
    of node: AttributeSyntax,
    attachedTo declaration: some DeclGroupSyntax,
    providingExtensionsOf type: some TypeSyntaxProtocol,
    conformingTo protocols: [TypeSyntax],
    in context: some MacroExpansionContext
  ) throws -> [ExtensionDeclSyntax] {
    let typeName = type.trimmedDescription

    var generatedMembers: [String] = []

    // Check if 'name' is already defined in the struct
    let hasName = declaration.memberBlock.members.contains { member in
      guard let varDecl = member.decl.as(VariableDeclSyntax.self) else { return false }
      return varDecl.bindings.contains { binding in
        binding.pattern.as(IdentifierPatternSyntax.self)?.identifier.text == "name" || binding.pattern.as(IdentifierPatternSyntax.self)?.identifier.text == "workflowType"
      }
    }

    if !hasName {
      let kebabName = typeName.camelCaseToKebabCase()
      generatedMembers.append("public static var name: String { \"\(kebabName)\" }")
    }

    let workflowExtension: DeclSyntax =
      """
      extension \(type.trimmed): WorkflowProtocol {
          \(raw: generatedMembers.joined(separator: "\n"))
      }
      """

    guard let extensionDecl = workflowExtension.as(ExtensionDeclSyntax.self) else {
      return []
    }

    return [extensionDecl]
  }
}

extension String {
  func camelCaseToKebabCase() -> String {
    let regex = try! NSRegularExpression(pattern: "([a-z0-9])([A-Z])", options: [])
    let range = NSRange(location: 0, length: self.utf16.count)
    var result = regex.stringByReplacingMatches(in: self, options: [], range: range, withTemplate: "$1-$2")
    result = result.replacingOccurrences(of: "Workflow", with: "")
    if result.hasSuffix("-") {
      result.removeLast()
    }
    return result.lowercased()
  }
}

public struct ActivityMacro: PeerMacro {
  public static func expansion(
    of node: AttributeSyntax,
    providingPeersOf declaration: some DeclSyntaxProtocol,
    in context: some MacroExpansionContext
  ) throws -> [DeclSyntax] {
    []
  }
}

public struct ActivityContainerMacro: MemberMacro, ExtensionMacro {
  public static func expansion(
    of node: AttributeSyntax,
    attachedTo declaration: some DeclGroupSyntax,
    providingExtensionsOf type: some TypeSyntaxProtocol,
    conformingTo protocols: [TypeSyntax],
    in context: some MacroExpansionContext
  ) throws -> [ExtensionDeclSyntax] {
    let containerExtension: DeclSyntax =
      """
      extension \(type.trimmed): ActivityContainerProtocol {}
      """

    guard let extensionDecl = containerExtension.as(ExtensionDeclSyntax.self) else {
      return []
    }

    return [extensionDecl]
  }

  public static func expansion(
    of node: AttributeSyntax,
    providingMembersOf declaration: some DeclGroupSyntax,
    conformingTo protocols: [TypeSyntax],
    in context: some MacroExpansionContext
  ) throws -> [DeclSyntax] {
    var activityCases: [String] = []
    var activityWrappers: [String] = []

    for member in declaration.memberBlock.members {
      guard let funcDecl = member.decl.as(FunctionDeclSyntax.self) else { continue }

      let hasActivityAttribute = funcDecl.attributes.contains { attr in
        guard let attribute = attr.as(AttributeSyntax.self),
          let type = attribute.attributeName.as(IdentifierTypeSyntax.self)
        else {
          return false
        }
        return type.name.text == "Activity"
      }

      if hasActivityAttribute {
        let funcName = funcDecl.name.text
        let structName = funcName.prefix(1).uppercased() + funcName.dropFirst()

        let inputType = funcDecl.signature.parameterClause.parameters.first?.type.description ?? "DurableVoid"
        let returnType = funcDecl.signature.returnClause?.type.description
        let isVoidReturn = returnType == nil || returnType == "Void" || returnType == "()"
        let outputType = isVoidReturn ? "DurableVoid" : (returnType ?? "DurableVoid")

        let callAndEncode =
          isVoidReturn
          ? "try await self.\(funcName)(input: input, context: context); return try encoder.encode(DurableVoid())"
          : "let output = try await self.\(funcName)(input: input, context: context); return try encoder.encode(output)"

        activityCases.append(
          """
          case "\(funcName)":
              let input = try decoder.decode(\(inputType).self, from: invocation.inputData)
              let context = ActivityContext(workflowID: invocation.workflowID, activityName: "\(funcName)", system: system)
              \(callAndEncode)
          """
        )

        activityWrappers.append(
          """
          public enum \(structName): ActivityReference {
              public typealias Input = \(inputType)
              public typealias Output = \(outputType)
              public static var name: String { "\(funcName)" }
          }
          """
        )
      }
    }

    let initDecl: DeclSyntax = "public init() {}"

    let handleFunc: DeclSyntax =
      """
      public func handle(invocation: ActivityInvocation, on system: ClusterSystem) async throws -> Data {
          let decoder = JSONDecoder()
          decoder.userInfo[.actorSystemKey] = system
          let encoder = JSONEncoder()
          encoder.userInfo[.actorSystemKey] = system

          switch invocation.name {
          \(raw: activityCases.joined(separator: "\n"))
          default:
              throw WorkflowRuntimeError.unknownActivityFailure("Activity \\(invocation.name) not found")
          }
      }
      """

    let activitiesEnum: DeclSyntax =
      """
      public enum Activities {
          \(raw: activityWrappers.joined(separator: "\n"))
      }
      """

    return [initDecl, handleFunc, activitiesEnum]
  }
}

@main
struct DurableWorkflowsPlugin: CompilerPlugin {
  let providingMacros: [Macro.Type] = [
    WorkflowMacro.self,
    ActivityMacro.self,
    ActivityContainerMacro.self,
  ]
}
