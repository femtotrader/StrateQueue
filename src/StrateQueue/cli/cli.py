"""
Main CLI Entry Point

Lightweight CLI entry point that uses the modular command system.
This replaces the monolithic cli.py with a clean, modular approach.
"""

import argparse
import sys
from pathlib import Path

# Load environment variables from .env file and user credentials
from dotenv import load_dotenv

load_dotenv()  # Load from current directory
load_dotenv(Path.home() / ".stratequeue" / "credentials.env")  # Load user credentials

from .command_factory import create_command, get_supported_commands
from .utils import get_cli_logger, setup_logging
from .utils.color_formatter import (
    create_enhanced_help_epilog,
    format_help_header,
    format_welcome_message,
)
from .utils.command_help import get_command_help
from .utils.enhanced_parser import EnhancedArgumentParser

# Import command registry to ensure commands are registered

logger = get_cli_logger('main')


def _load_test_stubs_if_needed() -> None:
    """Load test stubs if SQ_TEST_STUB_BROKERS environment variable is set"""
    import os
    
    if os.getenv("SQ_TEST_STUB_BROKERS") != "1":
        return
        
    try:
        # Add tests directory to Python path
        import sys
        import pathlib
        
        # Find the project root by looking for pyproject.toml
        current_dir = Path(__file__).resolve()
        project_root = None
        for parent in current_dir.parents:
            if (parent / "pyproject.toml").exists():
                project_root = parent
                break
        
        if not project_root:
            return  # Can't find project root, skip stub loading
            
        tests_dir = project_root / "tests"
        if tests_dir.exists() and str(tests_dir) not in sys.path:
            sys.path.insert(0, str(tests_dir))
        
        # Import the stub modules to register them in sys.modules
        import importlib
        importlib.import_module("unit_tests.brokers.alpaca.alpaca_stubs")
        importlib.import_module("unit_tests.brokers.ibkr.ibkr_stubs")
        
        # Register broker stubs using the same approach as sitecustomize.py
        import types
        
        def _register_stub(module_name: str, cls_name: str) -> None:
            stub_mod = types.ModuleType(module_name)
            
            class _StubBroker:
                def __init__(self, *_, **__):
                    pass
                
                def get_broker_info(self):
                    import types
                    name_map = {
                        "AlpacaBroker": "Alpaca-Stub",
                        "IBKRBroker": "IBKR-Stub"
                    }
                    return types.SimpleNamespace(
                        name=name_map.get(cls_name, f"stub-{cls_name.lower()}"),
                        version="0",
                        supported_features={},
                        description="stub",
                        supported_markets=["stocks"],
                        paper_trading=True,
                    )
                
                def validate_credentials(self) -> bool:
                    return True
            
            setattr(stub_mod, cls_name, _StubBroker)
            sys.modules[module_name] = stub_mod
            
            # Make sure parent packages expose the sub-module attribute
            parent, _, child = module_name.rpartition(".")
            if parent not in sys.modules:
                sys.modules[parent] = types.ModuleType(parent)
            setattr(sys.modules[parent], child, stub_mod)
        
        _register_stub("StrateQueue.brokers.Alpaca.alpaca_broker", "AlpacaBroker")
        _register_stub("StrateQueue.brokers.IBKR.ibkr_broker", "IBKRBroker")
        
    except Exception:
        # Silently continue if stub loading fails
        pass


def create_main_parser() -> argparse.ArgumentParser:
    """
    Create the main argument parser with subcommands

    Returns:
        Configured ArgumentParser
    """
    # Get supported commands for enhanced help
    supported_commands = get_supported_commands()

    parser = argparse.ArgumentParser(
        prog='stratequeue',
        description=format_help_header(),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog=create_enhanced_help_epilog(supported_commands)
    )

    # Global arguments
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 0.0.1'
    )

    # Create subparsers for commands (hide the auto-generated list, show enhanced version in epilog)
    subparsers = parser.add_subparsers(
        dest='command',
        help=argparse.SUPPRESS,  # Hide the auto-generated command list
        metavar=''  # Remove "{command} ..." footer
    )

    # Register command parsers
    register_command_parsers(subparsers)

    return parser


def register_command_parsers(subparsers) -> None:
    """
    Register parsers for all available commands

    Args:
        subparsers: Subparsers object to add command parsers to
    """
    supported_commands = get_supported_commands()

    for command_name, _description in supported_commands.items():
        command = create_command(command_name)
        if command:
            # Create subparser for this command with aliases
            aliases = []
            if hasattr(command, 'aliases'):
                aliases_attr = command.aliases
                # Ensure aliases is a list
                if isinstance(aliases_attr, list):
                    aliases = aliases_attr
                elif aliases_attr is not None:
                    aliases = [aliases_attr] if isinstance(aliases_attr, str) else []

            # Get enhanced help content
            enhanced_help = get_command_help(command_name)

            # Hide individual command help since we show enhanced version in epilog
            # Use enhanced parser for individual command help
            subparser = subparsers.add_parser(
                command_name,
                aliases=aliases,
                help=argparse.SUPPRESS,  # Hide from auto-generated list
                description=enhanced_help['description'],
                epilog=enhanced_help['epilog'],
                formatter_class=EnhancedArgumentParser().formatter_class
            )

            # Let the command configure its parser
            command.setup_parser(subparser)





def show_welcome_message() -> None:
    """Show welcome message when no command is provided"""
    supported_commands = get_supported_commands()
    print(format_welcome_message(supported_commands))


def main(argv: list[str] | None = None) -> int:
    """
    Main CLI entry point

    Args:
        argv: Command line arguments (default: sys.argv[1:])

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    # Load test stubs if in test mode
    _load_test_stubs_if_needed()
    
    try:
        # Parse arguments
        parser = create_main_parser()
        args = parser.parse_args(argv)

        # Setup logging
        setup_logging(args.verbose)
        logger.debug(f"CLI started with args: {args}")

        # Handle no command provided
        if not args.command:
            show_welcome_message()
            return 0

        # Get and execute command
        command = create_command(args.command)
        if not command:
            print(f"❌ Unknown command: {args.command}")
            print(f"💡 Available commands: {', '.join(get_supported_commands().keys())}")
            return 1

        # Execute command
        return command.run(args)

    except KeyboardInterrupt:
        print("\n⚠️  Operation cancelled by user")
        return 130  # Standard exit code for SIGINT

    except Exception as e:
        logger.exception(f"Unexpected error in main CLI: {e}")
        print(f"❌ Unexpected error: {e}")
        print("💡 Run with --verbose for detailed error information")
        return 1


if __name__ == "__main__":
    sys.exit(main())
